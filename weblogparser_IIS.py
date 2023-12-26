import sqlite3
import time

def create_database():
    output_name = f'weblogresult/result_weblog_{time.time()}.db'
    conn = sqlite3.connect(output_name)
    cursor = conn.cursor()
    sql = f"CREATE TABLE IIS_log(Local_IP text, Client_IP text, Date text, Method text, URL text, Argument text, Port text, Status_Code text, Size text, User_Agent text, Refer text, File_Name text)"
    cursor.execute(sql)
    return cursor, conn

def log_parse(contents, input_val, cursor):
    try:
        tmp_split = contents.split(' ')

        file_name = input_val.split('\\')[-1]
        val_date = tmp_split[0] + ' ' + tmp_split[1]
        val_local_ip = tmp_split[2]
        val_method = tmp_split[3]
        val_url = tmp_split[4]
        val_url_argu = tmp_split[5]
        val_port = tmp_split[6]
        val_client_ip = tmp_split[8]
        val_useragent = tmp_split[9]
        val_refer = tmp_split[10]
        val_status = tmp_split[11]
        val_size = tmp_split[14]

    except Exception as e:
        with open(f"{file_name}_parsing_error", 'a') as f2:
            f2.write(f"split error({e}): {contents}")
        return
    sql = f"INSERT INTO IIS_log(Local_IP, Client_IP, Date, Method, URL, Argument, Port, Status_Code, Size, User_Agent, Refer, File_Name) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    val = (
        val_local_ip, val_client_ip, val_date, val_method, val_url, val_url_argu, val_port, val_status, val_size,
        val_useragent, val_refer, file_name)
    cursor.execute(sql, val)

def read_file_contents(input_val, cursor):
    try:
        with open(input_val, 'r', encoding="utf-8", errors="ignore") as file:
            while True:
                contents = file.readline()
                if not contents:
                    break
                log_parse(contents, input_val, cursor)
    except Exception as e:
        print("An error occurred:", e)

def main():
    input_val = "/Users/*user*/pycodes/weblogtypes/type1.log"
    cursor, conn = create_database()
    read_file_contents(input_val, cursor)
    conn.commit()


if __name__ == "__main__":
    main()
