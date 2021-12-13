import sys
import mysql.connector
from mysql.connector import errorcode
import tkinter as tk


# I am going to establish a connection to the mysql instance that l created with the docker-compose
def get_db_connection():
    try:
        return mysql.connector.connect(host="localhost", user="root", password="pass_wordsky")
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Please check the password and try again")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Please ensure that the database does exist")
        else:
            print(err)

# Create the required qa_tests table
create_table_qa_tests = """
    CREATE TABLE IF NOT EXISTS qa_tests (
    id INT NOT NULL AUTO_INCREMENT,
    code VARCHAR(100), 
    description VARCHAR(150), 
    enabled VARCHAR(1), 
    parameter VARCHAR(1000),
    test_sql VARCHAR(1000),
    exp_result INT,
    PRIMARY KEY (id)
    )"""

# Create the required channel_table_env table
create_table_channel_table = """
    CREATE TABLE IF NOT EXISTS channel_table_env (    
    channel_code VARCHAR(100), transaction_date VARCHAR(100)
    )"""

# Create the required channel_transaction_env table
create_table_transaction_table = """
    CREATE TABLE IF NOT EXISTS channel_transaction_env (    
    channel_code VARCHAR(100), transaction_date VARCHAR(100),transaction_amount VARCHAR(10)
    )"""

# Set the trigger to automate code column value insertion for qa_tests
create_code_trigger = """
CREATE TRIGGER generate_code 
BEFORE INSERT ON qa_tests FOR EACH ROW
BEGIN        
    SET @MAX_ID = (SELECT MAX(id) from qa_tests);
    IF @MAX_ID IS NULL THEN
    SET @MAX_ID =0;
    END IF;

    SET NEW.code = CONCAT('qa_ch_0',CAST(@MAX_ID+1 as CHAR(50)));
END;
"""

# Insert into table
def insert_into_channel_table(db_con, val):
    with db_con.cursor() as mycursor:
        mycursor.execute(f"""INSERT INTO channel_table_env (channel_code,transaction_date) VALUES('{val}','date')""")
        db_con.commit()

# Insert into the transaction table
def insert_into_transaction_table(db_con, val):
    with db_con.cursor() as mycursor:
        mycursor.execute(f"""INSERT INTO channel_transaction_env (channel_code,transaction_date,transaction_amount) 
                            VALUES('{val}','date',1) """)
        db_con.commit()

# Insert into the qa_tests
def insert_into_qa_tests(db_con, val):
    insert_sql = """ INSERT INTO qa_tests (description,enabled,parameter,test_sql,exp_result) 
                      VALUES(%s,%s,%s,%s,%s) """
    with db_con.cursor() as mycursor:
        mycursor.execute(insert_sql, val)
        db_con.commit()


def initiate_qa_db(db_name):
    with db_con.cursor() as mycursor:
        mycursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        mycursor.execute(f"USE {db_name}")
        mycursor.execute("DROP TABLE IF EXISTS qa_tests")
        mycursor.execute("DROP TRIGGER IF EXISTS generate_code;")
        mycursor.execute(create_table_qa_tests)
        mycursor.execute(create_code_trigger)

        mycursor.execute("DROP TABLE IF EXISTS channel_table_env")
        mycursor.execute(create_table_channel_table)
        mycursor.execute("DROP TABLE IF EXISTS channel_transaction_env")
        mycursor.execute(create_table_transaction_table)
        db_con.commit()


def run_sql_test(db_con, code, **kwargs):
    with db_con.cursor(dictionary=True, buffered=True) as mycursor:
        test_result = ""
        error = None
        qa_check_flag = ""
        mycursor.execute(f"SELECT * FROM qa_tests where code='{code}'")
        data = mycursor.fetchall()
        param_list = data[0]['parameter'].replace(" ", "").split(
            ",")
        sql_to_exec = data[0]['test_sql']
        exp_result = data[0]['exp_result']
        for param in param_list:
            sql_to_exec = sql_to_exec.replace(f"**{param}**", kwargs[param])

        with db_con.cursor(dictionary=True, buffered=True) as mycursor:
            try:
                mycursor.execute(sql_to_exec)
                test_result = mycursor.fetchall()
            except mysql.connector.Error as err:
                error = err

        # Output to the required console
        if error is None:
            print(f"This is an error ##### {error}")
            sql_test_result = [val for val in test_result[0].values()][0]
            if exp_result == sql_test_result:
                qa_check_flag = "Passed"
            else:
                qa_check_flag = "Failed"
            print(f"QA Test Code ==> {data[0]['code']}\n")
            print(f"SQL executed for test ==> {sql_to_exec}\n")
            print(f"QA Test result ==> {sql_test_result}\n")
            print(f"QA Test Passed/Failed ==> {qa_check_flag}")
        else:
            print(f"Test failed due to error\n {error}")

        # Result GUI set up
        if error is None:
            window = tk.Tk()
            tk.Label(text="SQL QA Check", foreground="white", background="black").pack()
            tk.Label(text="").pack()  # adding blank line
            tk.Label(text=f"SQL Test Code: {data[0]['code']}\n\n").pack()
            tk.Label(text=f"SQL executed for test", foreground="white", background="black").pack()
            tk.Label(text=f"SQL executed for test {sql_to_exec}\n\n").pack()
            tk.Label(text=f"QA Test result", foreground="white", background="black").pack()
            tk.Label(text=f"{sql_test_result}\n\n").pack()
            tk.Label(text=f"QA Test Passed/Failed", foreground="white", background="black").pack()
            tk.Label(text=f"{qa_check_flag}\n\n").pack()
            tk.Button(text="Close", width=15, height=2, bg="blue", fg="white",
                      command=window.destroy).pack()
            window.mainloop()
        else:
            window = tk.Tk()
            tk.Label(text="SQL QA Check", foreground="white", background="black").pack()
            tk.Label(text="").pack()  # adding blank line
            tk.Label(text=f"SQL Test Code: {data[0]['code']}\n\n").pack()
            tk.Label(text=f"Test failed due to error", foreground="white", background="black").pack()
            tk.Label(text=f"{error}\n\n").pack()
            tk.Button(text="Close", width=15, height=2, bg="blue", fg="white",
                      command=window.destroy).pack()
            window.mainloop()


# Trigger mysql conn and Initiating qa_checks db in mysql
db_con = get_db_connection()
initiate_qa_db("qa_checks")

# Insert the test records in qa_tests table
#Code=qa_ch_01
insert_test1 = ('Runs the SQL against the Channel table to count duplicates. Duplicates count must be 0', 'Y', 'env', """Select count(*) from (select 
channel_code, count(*) from channel_table_env group by channel_code having count(*) > 1) AS X""", 0)
insert_into_qa_tests(db_con, insert_test1)

#Code=qa_ch_02
insert_test2 = (
'Check the FK between channel_code and its child table channel_transaction to identify orphans at a given date', 'Y',
'env, date', """select count(*) from channel_transaction_env A
left join channel_table_env B on (A.channel_code = B.channel_code) 
where B.channel_code is null and B.transaction_date = 'date'""", 0)

#Code=qa_ch_03
insert_into_qa_tests(db_con, insert_test2)
insert_test3 = ('Counts the records in channel_transaction table at a given date that have amount null', 'N', 'date', """select count(*) from channel_transaction_env 
where transaction_date = 'date' and transaction_amount is null""", 0)
insert_into_qa_tests(db_con, insert_test3)

# Insert the test records into channel table
insert_channel_type_data = ('val1')
insert_into_channel_table(db_con, insert_channel_type_data)
insert_channel_type_data = ('val2')
insert_into_channel_table(db_con, insert_channel_type_data)
insert_channel_type_data = ('val3')
insert_into_channel_table(db_con, insert_channel_type_data)

# Insert the test records into transaction table
insert_channel_type_data = ('val1')
insert_into_transaction_table(db_con, insert_channel_type_data)
insert_channel_type_data = ('val2')
insert_into_transaction_table(db_con, insert_channel_type_data)
insert_channel_type_data = ('val3')
insert_into_transaction_table(db_con, insert_channel_type_data)

# Trigger all QA checks
# qa_ch_01
run_sql_test(db_con, "qa_ch_01", env="env")
# qa_ch_02
run_sql_test(db_con, "qa_ch_02", env="env",date='date')
# qa_ch_03
run_sql_test(db_con, "qa_ch_03", date="date")