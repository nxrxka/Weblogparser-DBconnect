import sqlite3
import time
import apache_log_parser

def create_database():
    output_name = f'weblogresult/result_weblog_{time.time()}.db'
    conn = sqlite3.connect(output_name)
    cursor = conn.cursor()
    sql = f'CREATE TABLE apache_log(ip text,timestamp text, method text, request text, status_code text, byte int, file_name text)'
    cursor.execute(sql)
    return cursor, conn


def apache_log(contents, full_path, cursor):
    log_format = "%h %l %u %t \"%r\" %>s %b"
    line_parser = apache_log_parser.make_parser(log_format)

    try:
        val_ip = line_parser(contents)['remote_host']
        val_timestamp = line_parser(contents)['time_received_isoformat'].replace('T', ' ')
        val_method = line_parser(contents)['request_method']
        val_request = line_parser(contents)['request_url']
        val_status_code = line_parser(contents)['status']
        val_byte = line_parser(contents)['response_bytes_clf']


        file_name = full_path.split('\\')[-1]

    except Exception as e:
        with open(f"{file_name}_parsing_error", 'a') as f2:
            f2.write(f"split error({e}): {contents}")
        return
    sql = f"INSERT INTO apache_log(ip, timestamp, method, request, status_code, byte, file_name) VALUES (?, ?, ?, ?, ?, ?, ?)"
    val = (val_ip, val_timestamp, val_method, val_request, val_status_code, val_byte, file_name)
    cursor.execute(sql, val)


def read_file_contents(input_val, cursor):
    try:
        with open(input_val, 'r', encoding="utf-8", errors="ignore") as file:
            while True:
                contents = file.readline()
                if not contents:
                    break
                apache_log(contents, input_val, cursor)
    except Exception as e:
        print("An error occurred:", e)

def main():
    input_val = "/Users/*user*/pycodes/weblogtypes/type2.log"
    cursor, conn = create_database()
    read_file_contents(input_val, cursor)
    conn.commit()

if __name__ == "__main__":
    main()
