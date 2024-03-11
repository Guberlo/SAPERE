import confuse 

class Config:

    def __init__(self):
        
        self.config = confuse.Configuration('snitch', __name__)
        self.config.set_file('config/application-dev.yaml')
        self.bot_token = self.config["bot"]["token"].get()
        self.no_link_message = self.config["common"]["noLinkMessage"].get()
        self.error_message = self.config["common"]["errorMessage"].get()
        self.not_found_message = self.config["common"]["notFoundMessage"].get()
        self.es_host = self.config["elasticsearch"]["host"].get()
        self.es_port = self.config["elasticsearch"]["port"].get()
        self.es_index = self.config["elasticsearch"]["index"].get()
        self.kafka_host = self.config["kafka"]["host"].get()
        self.kafka_port = self.config["kafka"]["port"].get()
        self.kafka_topic = self.config["kafka"]["topic"].get()