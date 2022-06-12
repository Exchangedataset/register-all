import os
import sys
import boto3
import gzip
import MySQLdb

import config

def main():
    if os.path.exists(os.path.join(sys.argv[1], "stop")):
        exit(1)
    open(os.path.join(sys.argv[1], "stop"), "a").close()

    # connect to mysql server
    db = MySQLdb.connect(
        host=config.DATABASE_HOST,
        user=config.DATABASE_USER,
        passwd=config.DATABASE_PASSWORD,
        db=config.DATABASE_NAME,
        ssl={'ca': config.SSL_CA}
    )
    cur = db.cursor()

    # s3 client
    s3_client = boto3.client("s3")

    # get exchange directory
    files = os.listdir(sys.argv[1])
    files = sorted(files)

    last_file = None
    for file in files:
        path = os.path.join(sys.argv[1], file)

        print("checking ", file)

        first_line = None
        last_line = None
        with gzip.open(path) as f:
            first_line = f.readline()
            while True:
                line = f.readline()
                if line == "":
                    last_line = line
                else:
                    break

        # no err then upload last file
        if last_file is not None:
            print("upload", last_file)
            with open(file, 'rb') as f:
                s3_client.upload_file(f, config.S3_BUCKET, file)
                # store it to database
                cur.execute("INSERT INTO datasets VALUES(?, ?, ?, ?, ?)", file, exchange, start_nanosec, end_nanosec, is_head)
            os.remove(last_file)

        exchange = file.split("_")[0]
        split = first_line.split("\t")
        start_nanosec = int(split[1])
        is_head = split[0] == "head"
        end_nanosec = int(last_line.split("\t")[1])

    db.close()
    os.remove(os.path.join(sys.argv[1], "stop"))

if __name__ == "__main__":
    main()