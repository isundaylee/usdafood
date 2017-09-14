import sys

def usage():
    print('Usage: python get_list.py API_KEY')

if len(sys.argv) < 2:
    usage()
    exit()

API_KEY = sys.argv[1]
