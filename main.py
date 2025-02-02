from pipelines.steam_extractor import get_app_data
from pipelines.steam_loader import load_data


def main():
    get_app_data()
    load_data()


if __name__ == "__main__":
    main()
