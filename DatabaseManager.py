import sqlite3
class DBManager:
    def __init__(self,db_path):
        self.db_path = db_path
    def createTable(self,table_name, column_name, column_type):
        try:
            connection = sqlite3.connect(self.db_path)
            cursor = connection.cursor()
            column_vals = []
            for col_name,col_type in zip(column_name,column_type):
                column_vals.append("{} {}".format(col_name, col_type))
            column_vals = ",".join(column_vals)
            cursor.execute('''CREATE TABLE '''+table_name+'''('''+column_vals+''')''')
            connection.commit()
            connection.close()
        except sqlite3.Error as e:
            print("Error Occured in creating table ",e.args[0])
    def insertMany(self, insert_command, values_list):
        try:
            connection = sqlite3.connect(self.db_path)
            cursor = connection.cursor()
            cursor.executemany(insert_command,values_list)
            connection.commit()
            connection.close()
        except sqlite3.Error as e:
            print("Error Occured in inserting values ",e.args[0])
    def fetchRows(self, select_command):
        try:
            connection = sqlite3.connect(self.db_path)
            cursor = connection.cursor()
            rows = cursor.execute(select_command)
            rows = rows.fetchall()
            connection.close()
            return rows
        except sqlite3.Error as e:
            print("Error Occured in fetching rows ",e.args[0])
            return None
    def fetchRowsWithWhere(self, select_command,where_id):
        try:
            connection = sqlite3.connect(self.db_path)
            cursor = connection.cursor()
            rows = cursor.execute(select_command,where_id)
            rows = rows.fetchall()
            connection.close()
            return rows
        except sqlite3.Error as e:
            print("Error Occured in fetching rows ",e.args[0])
    def checkIfExists(self,select_command,where_id):
        try:
            connection = sqlite3.connect(self.db_path)
            cursor = connection.cursor()
            rows = cursor.execute(select_command,where_id)
            if rows.fetchall():
                return True
            return False
        except sqlite3.Error as e:
            print("Error Occured in checking if exists ",e.args[0])
    def dropTable(self,table_name):
        try:
            connection = sqlite3.connect(self.db_path)
            cursor = connection.cursor()
            cursor.execute("DROP TABLE " + table_name)
            connection.commit()
            connection.close()
        except sqlite3.Error as e:
            print("Error Occured in dropping table ",e.args[0])