import copy

from kaggle.api.kaggle_api_extended import KaggleApi
import kaggle
import kaggle.rest

kaggle.rest.ApiException.with_traceback


def find_latest_version(owner_slug: str, dataset_slug: str, version: int):
    curr_version = version + 1
    api = KaggleApi()
    api.authenticate()

    def is_version_valid(version: int):
        try:
            _ = api.datasets_list_files_with_http_info(
                owner_slug, dataset_slug, dataset_version_number=f"{version}"
            )
            return True
        except Exception:
            return False

    while is_version_valid(curr_version):
        curr_version += 1

    return curr_version - 1


print(find_latest_version("davidcariboo", "player-scores", 500))
