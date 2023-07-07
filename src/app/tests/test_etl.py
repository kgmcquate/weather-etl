def get_test_jdbc_options():
    sqlite_path = "tests/test.db"
    jdbc_url = f"jdbc:sqlite://{sqlite_path}"

    logger.debug(jdbc_url)

    return {
        "dbtable": lakes_table_name,
        "url": jdbc_url,
        "driver": "org.sqlite.JDBC"
    }


