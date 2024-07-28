from concurrent import futures
import grpc
import pipeline_compiler_service_pb2
import pipeline_compiler_service_pb2_grpc
from kfp import compiler
import tempfile
import os
import shutil
from kfp.cli import compile_
from pipeline_compiler_service_pb2 import CompilationResult
import sys
import re

class PipelineCompilerServiceServicer(pipeline_compiler_service_pb2_grpc.PipelineCompilerServiceServicer):
    def CompilePipeline(self, request, context):
        print("\nentering CompilePipeline")
        print(f"Server received: {request.data}")

        # decode incoming bytes to string
        string_data = request.data.decode('utf-8')

        # make sure the string is not empty
        string_data = string_data.strip()
        if not string_data:
            return pipeline_compiler_service_pb2.CompileReply(
                statusCode=CompilationResult.SYNTAX_ERROR,
                message=f"Uploaded file was detected to be an empty string ({len(request.data)} bytes of whitespace).")

        (temp_dir, pipeline_dsl_temp_file) = write_pipeline_dsl_to_file(string_data)
        print(f"compiling in temp dir {temp_dir} file {pipeline_dsl_temp_file}")
        result = compile(temp_dir, pipeline_dsl_temp_file)

        print("result type: ", type(result))

        # successful compilation
        if isinstance(result, str):
            print("result is a string: ", result)
            with open(result, 'rb') as f:
                data = f.read()
            shutil.rmtree(temp_dir)
            return pipeline_compiler_service_pb2.CompileReply(
                statusCode=CompilationResult.OK,
                message=f"incoming {len(request.data)} bytes. returning compiled pipeline {len(data)} bytes.",
                data=data)
        else:
            shutil.rmtree(temp_dir)
            (status_code, message) = handle_unsuccessful_compilation(result)
            return pipeline_compiler_service_pb2.CompileReply(
                statusCode=status_code,
                message=message)

def handle_unsuccessful_compilation(result):
        if isinstance(result, SyntaxError):
            print("result is a SyntaxError: ", result)
            error_message = clean_error_message(str(result))
            return (CompilationResult.SYNTAX_ERROR, error_message)
        elif isinstance(result, ValueError):
            print("result is a ValueError: ", result)
            error_message = clean_error_message(str(result))
            return (CompilationResult.SYNTAX_ERROR, error_message)
        elif isinstance(result, Exception):
            print("result is an Exception: ", result)
            error_message = clean_error_message(str(result))
            return (CompilationResult.EXCEPTION, error_message)
        else:
            print("result is unknown: ", result)
            return (CompilationResult.UNKNOWN, error_message)

def clean_error_message(error_message):
    if 'Expected one pipeline or one component in module' in error_message:
        return ("Could not find a pipeline or component in the uploaded file. "
            "Please make sure the file contains a pipeline function or component. "
            "(Did you forget to add a @dsl.pipeline or @dsl.component decorator?)")

    error_message = re.sub("[\\w\\-]*\\.py", "", error_message)
    error_message = re.sub(" <module.*>", "", error_message)
    error_message = error_message.replace("invalid syntax (, ", "Uploaded file had invalid syntax (")

    return error_message

def write_pipeline_dsl_to_file(pipeline_dsl):
    temp_dir = tempfile.mkdtemp()
    temp_file = tempfile.mktemp(dir=temp_dir, suffix='.py')
    with open (temp_file, 'w') as f:
        f.write(pipeline_dsl)
    return (temp_dir, temp_file)

def compile(dir, file):
    try:
        pipeline_func = compile_.collect_pipeline_or_component_func(python_file=file, function_name=None)
        # print(f"pipeline_func: {pipeline_func}")
        output_path = os.path.join(dir, 'pipeline.yaml')
        compiler.Compiler().compile(
            pipeline_func=pipeline_func,
            package_path=output_path,
            type_check=True)
    except Exception as e:
        return e
    finally:
        unload_module_for_pipeline_file(file)
    
    output_size = os.path.getsize(output_path)
    print(f"compiled pipeline to {output_path} ({output_size} bytes)")
    return output_path

def unload_module_for_pipeline_file(file):
    """
    The KFP compiler works by loading the pipeline file as a module and then compiling that
    to protobuf / yaml pipeline spec. One we have that yaml file, we don't need the module
    in memory. This function is called after the pipeline is compiled so we can unload the
    module and not leak memory.
    """
    print("attempting to unload compiled pipeline module from python runtime")
    module_to_unload = os.path.basename(file)
    module_to_unload = module_to_unload.replace(".py", "")
    if module_to_unload in sys.modules:
        print("found module: ", module_to_unload)
        print("sys.modules length = ", len(sys.modules))
        try:
            del sys.modules[module_to_unload]
        except Exception:
            print("failed to unload module: ", module_to_unload)
        print("sys.modules length = ", len(sys.modules))
    else:
        print("module not found: ", module_to_unload)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pipeline_compiler_service_pb2_grpc.add_PipelineCompilerServiceServicer_to_server(PipelineCompilerServiceServicer(), server)
    server.add_insecure_port('[::]:50051')
    print("listining on port 50051")
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    serve()
