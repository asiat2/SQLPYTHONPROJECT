import subprocess
import sys


def __isConda()-> bool:
    try:
        import conda
    except:
        is_conda = False
    else:
        is_conda = True

    return is_conda

def __isPip()-> bool:
    try:
        import pip
    except:
        is_pip = False
    else:
        is_pip = True

    return is_pip


def installModule(package):
    
    packageManager = "pip"

    if __isConda() == True:
        packageManager = "conda"
        subprocess.check_call([sys.executable, "-m", packageManager, "install", "-y", package])
    else:
        subprocess.check_call([sys.executable, "-m", packageManager, "install", package])

     
 
    