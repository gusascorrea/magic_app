from backend.services.fundamentus_data_service import update_fundamentus_history


def main():
    new_rows = update_fundamentus_history()
    print("Novos dados:", new_rows)


if __name__ == "__main__":
    main()
