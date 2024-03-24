from yaml import load
try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

def getYaml(path: str) -> dict:
    """
        Load the yaml configuration file into a dictionary.
        
        @param path: path to yaml file
    """
    with open(path) as yamlfile:
        cfg = load(yamlfile, Loader=Loader)

    print(type(cfg))
    return cfg