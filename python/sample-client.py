import grpc
import pipeline_compiler_service_pb2
import pipeline_compiler_service_pb2_grpc
import sys

def run():
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = pipeline_compiler_service_pb2_grpc.PipelineCompilerServiceStub(channel)

        with open(sys.argv[1], 'rb') as f:
            data = f.read()

        response = stub.CompilePipeline(pipeline_compiler_service_pb2.CompileRequest(data=data))

    if response.statusCode == pipeline_compiler_service_pb2.CompilationResult.OK:
        print(f"Successfully compiled! {response.message}")
    elif response.statusCode == pipeline_compiler_service_pb2.CompilationResult.SYNTAX_ERROR:
        print(f"Syntax error: {response.message}")
    elif response.statusCode == pipeline_compiler_service_pb2.CompilationResult.EXCEPTION:
        print(f"Exception: {response.message}")
    else:
        print(f"Unknown error: {response.message}")

    if response.data is not None and len(response.data) > 0:
        print(f"Received {len(response.data)} bytes")
        print("\n")
        print(response.data.decode('utf-8'))
        print("\n")

if __name__ == '__main__':
    run()
