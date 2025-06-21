from db_manager import InstagramDataManager

if __name__ == "__main__":
    mgr = InstagramDataManager()
    mgr.remove_duplicate_usernames()
    print("Duplicate usernames removed.")