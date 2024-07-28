from kfp import dsl


@dsl.component(base_image="docker.io/python:3.9.17")
def do_very_little():
    pass

@dsl.pipeline(name='baby-pipeline')
def baby_pipeline():
    t = do_very_little()
