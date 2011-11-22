import condor

def f(x):
    return x**2

def main():
    call = [(f, [x]) for x in range(16)]

    for (calls, result) in condor.do(calls, 4):
        print call.args, result

if __name__ == "__main__":
    main()

