from envs import APP_LOGGER_NAME

LOGGING_CONFIG = {
  "default": {
    "version": 1,
    "formatters": {
      "extend": {
              "format": "[%(asctime)s] [%(process)d] [%(levelname)s] [%(filename)s] [%(funcName)s]: %(message)s"
        }},
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": "DEBUG",
            "formatter": "extend",
            "stream": "ext://sys.stdout"
        }
    },
    "loggers": {
        APP_LOGGER_NAME: {
          "level": "DEBUG",
          "handlers": [
            "console"
          ]
        }
    }
  }
}
