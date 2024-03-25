import csv
from collections import Counter

# Read data from CSV file
def read_orders_from_csv(file_path):
    orders = []
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            item_id, customer_id, price = int(row[0]), int(row[1]), float(row[2])
            orders.append((item_id, customer_id, price))
    return orders

# Task 1: Show each customer with their spending total, sorted in decreasing order of spending total
def calculate_customer_spending(orders):
    customer_spending = {}
    for order in orders:
        _, customer_id, price = order
        customer_spending[customer_id] = customer_spending.get(customer_id, 0) + price
    return sorted(customer_spending.items(), key=lambda x: x[1], reverse=True)

# Task 2: Find the item id that was purchased most frequently
def most_frequent_item_id(orders):
    item_counts = Counter(order[0] for order in orders)
    return item_counts.most_common(1)[0][0]

# Task 3: Find the item id that has the highest price total
def item_id_highest_price_total(orders):
    item_prices = {}
    for order in orders:
        item_id, price = order[0], order[2]
        item_prices[item_id] = item_prices.get(item_id, 0) + price
    return max(item_prices, key=item_prices.get)

# Main function to execute the tasks
def main():
    orders = read_orders_from_csv('orders.csv')

    # Task 1
    print("Customer spending totals (sorted by decreasing order):")
    for customer_id, spending in calculate_customer_spending(orders):
        print(f"Customer ID: {customer_id}, Total Spending: ${spending:.2f}")

    # Task 2
    most_frequent_id = most_frequent_item_id(orders)
    print(f"\nItem ID purchased most frequently: {most_frequent_id}")

    # Task 3
    highest_price_total_id = item_id_highest_price_total(orders)
    print(f"Item ID with the highest price total: {highest_price_total_id}")

if __name__ == "__main__":
    main()
