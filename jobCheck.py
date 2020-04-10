#!/usr/bin/env python3
#Hello
import gzip
import subprocess
import sys
import argparse
import os
from sys import argv


class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


exitCode_dict = {'1:0'    : "Error",\
            '2:0'    : "Missuse of shell builtins",\
            '126:0'  : "Command invoked cannot execute",\
            '127:0'  : "Command not found",\
            '128:0'  : "Invalid argument to exit",\
            '129:0'  : "Fatal error",\
            '130:0'  : "Script terminated by Control+C",\
            '255\*:0': "Exit status out of range"}


def prepend_multiple_lines(file_name, list_of_lines):
    """Insert given list of strings as a new lines at the beginning of a file"""

    # define name of temporary dummy file
    dummy_file = file_name + '.bak'
    # open given original file in read mode and dummy file in write mode
    with open(file_name, 'r') as read_obj, open(dummy_file, 'w') as write_obj:
        # Iterate over the given list of strings and write them to dummy file as lines
        for line in list_of_lines:
            write_obj.write(line + '\n')
        # Read lines from original file one by one and append them to the dummy file
        for line in read_obj:
            write_obj.write(line)

    # remove original file
    os.remove(file_name)
    # Rename dummy file as the original file
    os.rename(dummy_file, file_name)

def jobCheck(jobID) :
    '''Launches sacct command and returns a dictionary with the specific values'''
    downRow = "Account,AllocCPUS,AllocNodes,AssocID,AveCPU,AveCPUFreq,AveDiskRead,AveDiskWrite,AvePages,AveRSS,MaxRSS,AveVMSize,Cluster,CPUTime,Elapsed,ExitCode,JobID,NodeList,Start,End,State,Submit,JobName,MaxVMSize" # THese are on the bottom row
    upperRow = "Partition%30,User,Timelimit,WorkDir%10000,ReqMem" # THese are on the row on top!

    # Extracting information from sacct from undeRow
    cmd = "sacct --format={info} -j {jobid} --units=M | awk 'NR != 2 && NR != 3' | tr -s ' '".format(info=downRow,jobid=jobID)
    jobInfo_under = subprocess.check_output(cmd,shell=True, universal_newlines=True).rstrip().split()
    dico = {jobInfo_under[i]:jobInfo_under[i+(len(jobInfo_under)//2)] for i in range(0,(len(jobInfo_under)//2))}

    # Extracting information from sacct from upperRow
    cmd_2 = "sacct --format={info} -j {jobid} --units=M | awk 'NR != 2 && NR != 4' | tr -s ' '".format(info=upperRow,jobid=jobID)
    jobInfo_upper = subprocess.check_output(cmd_2, shell=True, universal_newlines=True).rstrip().split()
    dico_2 = {jobInfo_upper[i]: jobInfo_upper[i + (len(jobInfo_upper)//2)] for i in range(0, (len(jobInfo_upper)//2))}
    dico.update(dico_2)

    return dico


def get_jobID(logFile) :
    '''This function reads the log file and extract the jobID'''
    jobID = ""
    if logFile.split("-")[0] == "slurm" :
        jobID = logFile.split("-")[-1].replace(".out","")
    elif len(logFile.split("_")) > 0 :
        jobID = logFile.split("_")[-1].replace(".out","")
    else :
        raise Exception(f"{bcolors.WARNING}slurm job output format is not correct. Please you should write the output of sbatch using the this format : <jobName>_<jobID>.out\nIf you do not specify any output for slurm, it automatically generates a file slurm.<jobID>.out and you can use that one.\nOTHERWHISE Just specify the jobID and not the output of slurm.{bcolors.ENDC}")
    return jobID


def check_for_ID(logFile=None, jobID=None) :
    id = ""
    if logFile == None and jobID == None:
        raise Exception(f"{bcolors.WARNING}Please specify either a logfile or a jobID.{bcolors.ENDC}")
    elif logFile != None and jobID == None:
        return get_jobID(logFile)
    else:
        return str(jobID)

def check_for_command(logFile):
    '''
    In order to get command, we need to read the first lines of the log FIle and IF executed with wsbatch we should have two lines, one that gives the executed command and another one which is the entire sbatch command submitted.
    '''
    with open(logFile,"rt") as f:
        command = f.readline().rstrip()
        if command[:9] == "__WSBATCH" :
            return command.split('"')[1]
        else:
            return "Job was not submitted using wsbatch therefore no commands can be written. If you want to have command information, please use wsbatch."




def out(jobID,logFile) :

    dico = jobCheck(jobID)
    #### CHECKING WHETHER FIRST LINE OF LOG FILE HAS WSBATCH
    command = check_for_command(logFile)

    MaxRSS = dico.get('MaxRSS')[:-1]
    AveRSS = dico.get('AveRSS')[:-1]
    ReqMem = dico.get('ReqMem')[:-1]
    MaxVMSize = dico.get('MaxVMSize')[:-1]

    DeltMem = str(round(float(ReqMem[:-1]) - float(MaxRSS), 2))

    lst = []

    lst.append(f"{bcolors.HEADER}SLURM job summary{bcolors.ENDC}")
    lst.append(f"Subject : Job {jobID} in cluster <{dico.get('Cluster')}> Done\n")
    lst.append(f"Job <{dico.get('JobName')}> was submitted by user <{dico.get('User')}> in cluster <{dico.get('Cluster')}>.")
    lst.append(f"Job was executed on host(s) <{dico.get('NodeList')}>, in queue <{dico.get('Partition')}>, as user <{dico.get('User')}> in cluster <{dico.get('Cluster')}>.")
    lst.append(f"<{dico.get('WorkDir')}> was used as the working directory.")

    lst.append(f"Started at {dico.get('Start')}")
    lst.append(f"Results reported at {dico.get('End')}\n")

    lst.append(f"Your job looked like:")
    lst.append("\n------------------------------------------------------------\n")
    lst.append(f"{command}")
    lst.append("\n------------------------------------------------------------\n")

    if dico.get("ExitCode") == '0:0':
        lst.append(f"{bcolors.OKGREEN}Successfully completed with Exit code 0.{bcolors.ENDC}")
    else:
        lst.append(f"{bcolors.FAIL}FAILED with Exit code {dico.get('ExitCode')} : {exitCode_dict.get(dico.get('ExitCode'))}.{bcolors.ENDC}")

    lst.append(f"\nResource usage summary:\n\n")
    lst.append(f"CPU time :               {dico.get('CPUTime')}")
    lst.append(f"Max Memory (MaxVMSize) : {MaxVMSize} MB")
    lst.append(f"Max Memory (MaxRSS) :    {MaxRSS} MB")
    lst.append(f"Average Memory :         {AveRSS} MB")
    lst.append(f"Total Requested Memory : {ReqMem} MB")
    lst.append(f"Delta Memory:            {DeltMem}")
    lst.append(f"(Delta: the difference between total requested memory and actual max usage.)")
    lst.append(f"Max Swap :               -")
    lst.append("\n")
    lst.append(f"Max Processes :          -")
    lst.append(f"Max Threads :            -")
    #lst.append(f"\nThe output if any follows below:")
    lst.append("\n")

    return lst


def screen(logFile=None, jobID=None):
    ID = check_for_ID(logFile=logFile, jobID=jobID)
    info = out(ID)
    for i in info :
        print("".join(i))


def write(logFile=None, jobID=None, output=None) :
    g = open(output, "w")
    ID = check_for_ID(logFile=logFile, jobID=jobID)
    info = out(ID)
    for i in info :
        g.write("".join(i)+"\n")


def append_to_slurm_log(logFile):
    jobID = get_jobID(logFile)
    info = out(jobID,logFile)
    info.append(f"\nThe output if any follows below:")
    info.append("\n")
    prepend_multiple_lines(logFile,info)


'''
def main(logFile=None,jobID=None) :
    Main function
    id = check_for_ID(logFile=logFile, jobID=jobID)

    dico = jobCheck(id)

    MaxRSS = dico.get('MaxRSS')[:-1]
    AveRSS = dico.get('AveRSS')[:-1]
    ReqMem = dico.get('ReqMem')[:-1]
    MaxVMSize = dico.get('MaxVMSize')[:-1]
    DeltMem = str(round(float(ReqMem[:-1]) - float(MaxVMSize),2))

    print(f"{bcolors.HEADER}SLURM job summary{bcolors.ENDC}")
    print(f"Subject : Job {id} in cluster <{dico.get('Cluster')}> Done\n")
    print(f"Job <{dico.get('JobName')}> was submitted by user <{dico.get('User')}> in cluster <{dico.get('Cluster')}>.")
    print(f"Job was executed on host(s) <{dico.get('NodeList')}>, in queue <{dico.get('Partition')}>, as user <{dico.get('User')}> in cluster <{dico.get('Cluster')}>.")
    print(f"<{dico.get('WorkDir')}> was used as the working directory.")

    print(f"Started at {dico.get('Start')}")
    print(f"Results reported at {dico.get('End')}\n")

    print(f"Your job looked like:")
    print("\n------------------------------------------------------------\n")
    print("Here write the command executed. Have no fucking idea how to do that for the moment :).")
    print("\n------------------------------------------------------------\n")

    if dico.get("ExitCode") == '0:0' :
        print(f"{bcolors.OKGREEN}Successfully completed with Exit code 0.{bcolors.ENDC}")
    else :
        print(f"{bcolors.FAIL}FAILED with Exit code {dico.get('ExitCode')} : {exitCode_dict.get(dico.get('ExitCode'))}.{bcolors.ENDC}")

    print(f"\nResource usage summary:\n\n")
    print(f"CPU time :               {dico.get('CPUTime')}")
    print(f"Max Memory (MaxVMSize) : {MaxVMSize} MB")
    print(f"Max Memory (MaxRSS) :    {MaxRSS} MB")
    print(f"Average Memory :         {AveRSS} MB")
    print(f"Total Requested Memory : {ReqMem} MB")
    print(f"Delta Memory:            {DeltMem}")
    print(f"(Delta: the difference between total requested memory and actual max usage.)")
    print(f"Max Swap :               -")
    print()
    print(f"Max Processes :          -")
    print(f"Max Threads :            -")
    print(f"\nThe output should be on the logFile generated from SLURM.")

def main_write(logFile=None, jobID=None,output=None) :
    Main function
    g = open(output,"w")
    id = check_for_ID(logFile=logFile, jobID=jobID)


    dico = jobCheck(id)

    MaxRSS = dico.get('MaxRSS')[:-1]
    AveRSS = dico.get('AveRSS')[:-1]
    ReqMem = dico.get('ReqMem')[:-1]
    MaxVMSize = dico.get('MaxVMSize')[:-1]

    DeltMem = str(round(float(ReqMem[:-1]) - float(MaxRSS),2))

    g.write(f"{bcolors.HEADER}SLURM job summary{bcolors.ENDC}\n")
    g.write(f"Subject : Job {id} in cluster <{dico.get('Cluster')}> Done\n\n")
    g.write(f"Job <{dico.get('JobName')}> was submitted by user <{dico.get('User')}> in cluster <{dico.get('Cluster')}>.\n")
    g.write(f"Job was executed on host(s) <{dico.get('NodeList')}>, in queue <{dico.get('Partition')}>, as user <{dico.get('User')}> in cluster <{dico.get('Cluster')}>.\n")
    g.write(f"<{dico.get('WorkDir')}> was used as the working directory.\n")
    g.write(f"Started at {dico.get('Start')}\n")
    g.write(f"Results reported at {dico.get('End')}\n\n")
    g.write(f"Your job looked like:\n")
    g.write("\n------------------------------------------------------------\n\n")
    g.write("Here write the command executed. Have no fucking idea how to do that for the moment :).\n")
    g.write("\n------------------------------------------------------------\n\n")

    if dico.get("ExitCode") == '0:0' :
        g.write(f"{bcolors.OKGREEN}Successfully completed with Exit code 0.{bcolors.ENDC}\n")
    else :
        g.write(f"{bcolors.FAIL}FAILED with Exit code {dico.get('ExitCode')} : {exitCode_dict.get(dico.get('ExitCode'))}.{bcolors.ENDC}\n")

    g.write(f"\nResource usage summary:\n\n")
    g.write(f"CPU time :               {dico.get('CPUTime')}\n")
    g.write(f"Max Memory (MaxVMSize) : {MaxVMSize} MB\n")
    g.write(f"Max Memory (MaxRSS) :    {MaxRSS} MB\n")
    g.write(f"Average Memory :         {AveRSS} MB\n")
    g.write(f"Total Requested Memory : {ReqMem} MB\n")
    g.write(f"Delta Memory:            {DeltMem}\n")
    g.write(f"(Delta: the difference between total requested memory and actual max usage.)\n")
    g.write(f"Max Swap :               -\n")
    g.write(f"Max Processes :          -\n")
    g.write(f"Max Threads :            -\n")
    g.write(f"\nThe output should be on the logFile generated from SLURM.\n")


def append_to_slurm_log(logFile) :
    jobID = logFile.split("_")[-1].split(".")[0]
    dico = jobCheck(jobID)

    MaxRSS = dico.get('MaxRSS')[:-1]
    AveRSS = dico.get('AveRSS')[:-1]
    ReqMem = dico.get('ReqMem')[:-1]
    MaxVMSize = dico.get('MaxVMSize')[:-1]

    DeltMem = str(round(float(ReqMem[:-1]) - float(MaxRSS), 2))

    lst = []

    lst.append(f"{bcolors.HEADER}SLURM job summary{bcolors.ENDC}")
    lst.append(f"Subject : Job {id} in cluster <{dico.get('Cluster')}> Done\n")
    lst.append(f"Job <{dico.get('JobName')}> was submitted by user <{dico.get('User')}> in cluster <{dico.get('Cluster')}>.")
    lst.append(f"Job was executed on host(s) <{dico.get('NodeList')}>, in queue <{dico.get('Partition')}>, as user <{dico.get('User')}> in cluster <{dico.get('Cluster')}>.")
    lst.append(f"<{dico.get('WorkDir')}> was used as the working directory.")

    lst.append(f"Started at {dico.get('Start')}")
    lst.append(f"Results reported at {dico.get('End')}\n")

    lst.append(f"Your job looked like:")
    lst.append("\n------------------------------------------------------------\n")
    lst.append("Here write the command executed. Have no fucking idea how to do that for the moment :).")
    lst.append("\n------------------------------------------------------------\n")

    if dico.get("ExitCode") == '0:0':
        lst.append(f"{bcolors.OKGREEN}Successfully completed with Exit code 0.{bcolors.ENDC}")
    else:
        lst.append(f"{bcolors.FAIL}FAILED with Exit code {dico.get('ExitCode')} : {exitCode_dict.get(dico.get('ExitCode'))}.{bcolors.ENDC}")

    lst.append(f"\nResource usage summary:\n\n")
    lst.append(f"CPU time :               {dico.get('CPUTime')}")
    lst.append(f"Max Memory (MaxVMSize) : {MaxVMSize} MB")
    lst.append(f"Max Memory (MaxRSS) :    {MaxRSS} MB")
    lst.append(f"Average Memory :         {AveRSS} MB")
    lst.append(f"Total Requested Memory : {ReqMem} MB")
    lst.append(f"Delta Memory:            {DeltMem}")
    lst.append(f"(Delta: the difference between total requested memory and actual max usage.)")
    lst.append(f"Max Swap :               -")
    lst.append("\n")
    lst.append(f"Max Processes :          -")
    lst.append(f"Max Threads :            -")
    lst.append(f"\nThe output if any follows below:")
    lst.append("\n")

    prepend_multiple_lines(logFile,lst)

    return 0
'''

def quick_check(logFile=None, jobID=None) :
    '''Checks only the Exit Code of a file and return whether job(s) have successfully completed or not. Does not outputs any memory or CPU usage.'''
    id = check_for_ID(logFile=logFile, jobID=jobID)

    dico = jobCheck(id)

    ExitCode = dico.get("ExitCode")
    if ExitCode == "0:0" :
        print(f"{bcolors.OKGREEN}Successfully Completed{bcolors.ENDC} with exit code {ExitCode}")
    else :
        print(f"{bcolors.FAIL}Job FAILED{bcolors.ENDC} with exit code {ExitCode}")


parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,description='Job Check tool for slurm. Provides similar information as lsf output.')

subparsers = parser.add_subparsers(help="",dest="cmd",description="Available modes")

A_parser=subparsers.add_parser("screen",help="Outputs on screen job summary about the slurm job.")
A_IO = A_parser.add_argument_group('Main arguments')
A_IO.add_argument("-log","--logFile",
                    type=str,
                    nargs=1,
                    dest='logFile',
                    action="store",
                    default=None,
                    help="log file name")
A_IO.add_argument("-jobID","--jobID",
                    type=str,
                    nargs=1,
                    action="store",
                    dest="jobID",
                    default=None,
                    help="Job id (should be a number)")

B_parser=subparsers.add_parser("write",help="Writes the job summary into a new file.")
B_IO = B_parser.add_argument_group('Main arguments')
B_IO.add_argument("-log","--logFile",
                    type=str,
                    nargs=1,
                    dest='logFile',
                    action="store",
                    default=None,
                    help="log file name")
B_IO.add_argument("-jobID","--jobID",
                    type=str,
                    nargs=1,
                    action="store",
                    dest="jobID",
                    default=None,
                    help="Job id (should be a number)")
B_IO.add_argument("-out","--output",
                    type=str,
                    action="store",
                    dest="output",
                    default=None,
                    help="Output file to write job Summary")

C_parser=subparsers.add_parser("append",help="Appends the job summary into the begining of the slurm log file. BE CAREFULL if the log file is very big, might take a while because it rewrites the file into a new one and renames it after.")
C_IO = C_parser.add_argument_group('Main arguments')
C_IO.add_argument("-log","--logFile",
                    type=str,
                    nargs=1,
                    dest='logFile',
                    action="store",
                    default=None,
                    help="log file name")

D_parser=subparsers.add_parser("quick_check",help="Checks only for exit status of slurm job. Outputs if successfully completed or failed.")
D_IO = D_parser.add_argument_group('Main arguments')
D_IO.add_argument("-log","--logFile",
                    type=str,
                    nargs=1,
                    dest='logFile',
                    action="store",
                    default=None,
                    help="log file name")
D_IO.add_argument("-jobID","--jobID",
                    type=str,
                    nargs=1,
                    action="store",
                    dest="jobID",
                    default=None,
                    help="Job id (should be a number)")


args = parser.parse_args()

if __name__ == "__main__":
    if args.cmd == "screen" :
        if args.logFile != None and args.jobID == None :
            screen(logFile=args.logFile[0])
        elif args.logFile == None and args.jobID != None:
            screen(jobID=args.jobID[0])
        else :
            raise Exception("Please specify either a proper logFile name or proper jobID.")

    if args.cmd == "write" :
        if args.logFile == None and args.jobID != None and args.output != None:
            write(jobID=args.jobID[0], output=args.output)
        elif args.logFile != None and args.jobID == None and args.output != None:
            write(logFile=args.logFile[0], output=args.output)
        else :
            raise Exception("Please specify either a proper logFile name or proper jobID and and output file.")

    if args.cmd == "append":
        if args.logFile != None:
            append_to_slurm_log(args.logFile[0])
        else :
            raise Exception("Please specify either a proper logFile name.")

    if args.cmd == "quick_check":
        if args.logFile != None and args.jobID == None and args.output == None:
            quick_check(logFile=args.logFile[0])
        elif args.logFile == None and args.jobID != None and args.output == None:
            quick_check(jobID=args.jobID[0])
        else :
            raise Exception("Please specify either a proper logFile name or proper jobID.")


