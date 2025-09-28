import sqlite3


def init_db(db_path="audit.db"):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS audit")

    cur.execute(
        """
        CREATE TABLE audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            model TEXT,
            run_datetime TEXT,
            params TEXT,
            inference_time REAL,
            rmse_train REAL,
            mae_train REAL,
            r2_train REAL,
            rmse_val REAL,
            mae_val REAL,
            r2_val REAL,
            rmse_test REAL,
            mae_test REAL,
            r2_test REAL
        )
    """
    )

    cur.execute("DROP TABLE IF EXISTS clustering_audit")

    cur.execute(
        """
        CREATE TABLE clustering_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cluster INTEGER,
            model TEXT,
            run_datetime TEXT,
            params TEXT,
            inference_time REAL,
            rmse_train REAL,
            mae_train REAL,
            r2_train REAL,
            rmse_val REAL,
            mae_val REAL,
            r2_val REAL,
            rmse_test REAL,
            mae_test REAL,
            r2_test REAL
        )
    """
    )

    conn.commit()
    conn.close()


if __name__ == "__main__":
    init_db("db/audit.db")
    print("DB initialized")
