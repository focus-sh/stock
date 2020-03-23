from os import environ as env


class Environment:

    def home(self):
        return self.get_env_with_def('HOME', '/')

    @staticmethod
    def get_env_with_def(key, val):
        env_val = env.get(key)
        if env_val is not None:
            return env_val

        return val


environment = Environment()
