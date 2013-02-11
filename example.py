import condor

def f(x):
    return x**2

def main():
    calls = [(f, [x]) for x in range(16)]

    for (call, result) in condor.do(calls, 4):
        print call.args, result

if __name__ == "__main__":
    main()

