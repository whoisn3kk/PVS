class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

print(f"{bcolors.BOLD}[ {bcolors.WARNING}LOG{bcolors.ENDC}{bcolors.BOLD} ]{bcolors.ENDC} — counter reseted to {bcolors.BOLD}{bcolors.OKBLUE}0{bcolors.ENDC}\n")

with open('new_file.txt', 'w') as file:
    file.write(f"{bcolors.BOLD}[ {bcolors.WARNING}LOG{bcolors.ENDC}{bcolors.BOLD} ]{bcolors.ENDC} — counter reseted to {bcolors.BOLD}{bcolors.OKBLUE}0{bcolors.ENDC}\n")