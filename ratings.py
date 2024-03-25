def count_ratings(filename):
    ratings_count = [0] * 5
    total_ratings = 0

    with open(filename, 'r') as file:
        for line in file:
            _, _, rating, _ = map(int, line.strip().split())
            ratings_count[rating - 1] += 1
            total_ratings += 1

    return ratings_count, total_ratings


def print_results(ratings_count, total_ratings):
    print("Rating\t#rating \t\tPercentage")
    print("--------------------------------------")
    for i, count in enumerate(ratings_count):
        rating = i + 1
        percentage = (count / total_ratings) * 100 if total_ratings > 0 else 0
        print(f"{rating}\t{count}\t\t\t{percentage:.2f}%")

def main():
    filename = "u.data"
    ratings_count, total_ratings = count_ratings(filename)
    print_results(ratings_count, total_ratings)

if __name__ == "__main__":
    main()
