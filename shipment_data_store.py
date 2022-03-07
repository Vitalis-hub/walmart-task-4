import sqlite3
import csv
con = sqlite3.connect('shipment_database.db')
cur = con.cursor()
i = -1
b = -1
a = -1
with open('shipping_data_0.csv', newline='') as csvfile:
    reader = csv.reader(csvfile, delimiter=' ', quotechar='|')
    i+=1
    cur.execute("insert into shipment VALUES (?, ?, ?, ?, ?)", (i, i, reader[4], reader[0], reader[1]))
    con.commit()
    
with open('shipping_data_0.csv', newline='') as csvfile:
    reader = csv.reader(csvfile, delimiter=' ', quotechar='|')
    b+=1
    cur.execute("insert into product VALUES (?, ?)", (i, reader[2]))
    con.commit()


try:
    dict = {}
    with open('shipping_data_1.csv', newline='') as csvfile1, open('shipping_data_2.csv', newline='') as csvfile2:
        reader = csv.reader(csvfile1, delimiter=' ', quotechar='|')
        reader2 = csv.reader(csvfile2, delimiter=' ', quotechar='|')
        t = (reader[0], reader[1],reader2[1], reader2[2])
        dict[t] = dict[t] + 1
except:
    print("Cannot read files")
    
try:
    #here
    for key in dict:
        a += 1
        quantity = dict[key]
        orgin = key[2]
        dest = key[3]
        cur.execute("insert into shipment VALUES (?, ?, ?, ?, ?)", (a, a, quantity, orgin, dest))
        con.commit()
        cur.execute("insert into product VALUES (?, ?)", (a, reader[1]))
        con.commit()
except:
    print('Cannot insert row to database')

    
