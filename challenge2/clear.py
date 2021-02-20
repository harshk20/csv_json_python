import shutil
import os

def cleanUp() :
    if os.path.exists("challenge2/output") == False:
        os.mkdir("challenge2/output")
    shutil.rmtree("challenge2/output")
    os.mkdir("challenge2/output")