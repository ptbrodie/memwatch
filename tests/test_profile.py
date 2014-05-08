from memwatch import profile, ProfiledBlock


@profile("test.profile_block")
def profile_block():
    numbers = []
    for i in range(100000):
        for j in range(10):
            numbers.append([i] * j)


def main():
    with ProfiledBlock("test.outer_block"):
        for i in range(10):
            profile_block()


if __name__ == "__main__":
    main()
