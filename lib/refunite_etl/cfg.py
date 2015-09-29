"""Configuration
   @author: okello
   @created: 12-aug-2015

"""
import logging
import ConfigParser


class Cfg(object):
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.logger.info("Initialized config class...")
    
    def get_cfg(self, cfg_file_path):
        cfg =  ConfigParser.ConfigParser()
        cfg.read(cfg_file_path)
    
        return cfg

    def get_module(self, name):
        self.logger.debug("Calculated Module: %s" % name)
        mod = __import__(name)
        components = name.split('.')
        for comp in components[1:]:
            mod = getattr(mod, comp)
            
        return mod

    def get_class(self, class_fqn):
        self.logger.debug("FQN: %s" % class_fqn)
        comps = class_fqn.split(".")
                    
        class_name = comps[-1]
        module_name = class_fqn.rstrip(class_name).rstrip(".")
        self.logger.debug("Module: %s, Class: %s" % (module_name, class_name))
        
        module = self.get_module(module_name)
        _class = getattr(module, class_name)
        
        return _class