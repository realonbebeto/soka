from pydantic import BaseSettings

class Settings(BaseSettings):
    db_username: str
    db_password: str
    db_host: str
    db_port: str
    db_name: str
    dataset_id: str

    class Config:
        env_file = "/home/main/Documents/kazispaces/desrc/soka/.env"


settings = Settings()