import csv
import random
from faker import Faker
from datetime import datetime, timedelta
import os

fake = Faker()


def read_customer_data(master_csv):
    customer_data = {}
    with open(master_csv, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            customer_data[row['customer_id']] = {
                'name': row['name'],
                'debit_card_number': row['debit_card_number'],
                'debit_card_type': row['debit_card_type'],
                'bank_name': row['bank_name']
            }
    return customer_data


customer_data = read_customer_data('master_customer_data.csv')


def generate_transaction_data(num_records, amount_min=10, amount_max=1000):
    data = []
    for _ in range(num_records):
        customer_id = random.choice(list(customer_data.keys()))
        name = customer_data[customer_id]['name']
        debit_card_number = customer_data[customer_id]['debit_card_number']
        debit_card_type = customer_data[customer_id]['debit_card_type']
        bank_name = customer_data[customer_id]['bank_name']
        transaction_date = fake.date_this_year()
        amount_spent = round(random.uniform(amount_min, amount_max), 2)
        data.append([customer_id, name, debit_card_number, debit_card_type, bank_name, transaction_date, amount_spent])
    return data


def save_to_csv(data, filename):
    mode = 'a' if os.path.exists(filename) else 'w'
    with open(filename, mode, newline='') as file:
        writer = csv.writer(file)
        if mode == 'w':
            writer.writerow(
                ['customer_id', 'name', 'debit_card_number', 'debit_card_type', 'bank_name', 'transaction_date',
                 'amount_spent'])
        writer.writerows(data)


def main():
    num_records = 200
    num_days = 1
    start_date = datetime.now() + timedelta(days=num_days)

    for i in range(num_days):
        data = generate_transaction_data(num_records)
        today = (start_date + timedelta(days=i)).strftime("%Y-%m-%d")
        filename = f"transactions_{today}.csv"
        save_to_csv(data, filename)
        print(f"Generated {num_records} records for {today} and saved to {filename}")


if __name__ == "__main__":
    main()
