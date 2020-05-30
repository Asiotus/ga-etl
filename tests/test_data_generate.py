import json

def main():
    date = "20160803"

    openfilepath = 'data/ga_sessions_'+date+'.json'
    filepath = 'out/test' + date + ".json"
    i = 0
    outdict = {}
    with open(filepath, "w") as fout:
        with open (openfilepath, 'r') as fin:
            for line in fin:
                if i == 99:
                    fout.write(line)
                    print(line)
                    i = -1
                i = i+1


     
    # with open(filepath, "w") as fout:
    #     json.dump(outdict, fout)
    return None
if __name__ == '__main__': 
    main()
    print("end")