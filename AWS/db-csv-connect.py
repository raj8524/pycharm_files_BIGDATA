import csv
import pymysql

# csv file name
filename = "../Pyspark_data/Read_write-FILES-FORMATS/electronic.csv"

# initializing the titles and rows list
fields = []
rows = []

# reading csv file
with open(filename, 'r') as csvfile:
    # creating a csv reader object
    csvreader = csv.reader(csvfile)

    # extracting field names through first row
    fields = next(csvreader)

    # extracting each data row one by one
    for row in csvreader:
        rows.append(row)

    # get total number of rows
    print("Total no. of rows: %d" % (csvreader.line_num))


# printing the field names
print('Field names are:' + ', '.join(field for field in fields))
xy=""
xy=', '.join(field for field in fields)

#  printing first 5 rows
print('\nFirst 5 rows are:\n')

try:
        conn = pymysql.connect( user="username", passwd="password", db="db_name", connect_timeout=5)
    except pymysql.MySQLError as e:
        print("ERROR: Unexpected error: Could not connect to MySQL instance.")

    try:
        cur = conn.cursor()
        cur.execute("create table Employees ( id INT NOT NULL AUTO_INCREMENT, Name varchar(255) NOT NULL, PRIMARY KEY (id))")
        for row in rows[:5]:
            cur.execute(insert into  employees(xy) values(row))
            print(row)
        conn.commit()

    except:
        pass


