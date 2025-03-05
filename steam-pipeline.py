from pipelines.steam.steam_extractor import get_app_data
from pipelines.steam.steam_loader import load_data


def main():
    print("Getting app data.")
    get_app_data()
    print("loading data into schema.")
    load_data()


if __name__ == "__main__":
    main()
