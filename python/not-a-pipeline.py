from kfp import dsl


def do_very_little():
    pass

def baby_pipeline():
    t = do_very_little()
    print(t)
