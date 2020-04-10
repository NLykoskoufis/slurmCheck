#!/usr/bin/env python3

import gzip
import subprocess
import sys
import argparse
import os
from sys import argv
import re

#======================================================================================================================#
DESC_COMMENT = "Script to extract summary statistics about finished slurm jobs."
SCRIPT_NAME  = "jobCheck_v2.py"
#======================================================================================================================#

"""
#======================================================================================================================#
@author: Nikolaos M R Lykoskoufis
@date: 10/04/2020
@copyright: Copyright 2020, University of Geneva (UNIGE)
@information: Script to extract summary statistics about finished slurm jobs
#======================================================================================================================#
"""


class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

class JobID(object):
    def __init__(self,logFile):
        self.logFile = logFile

    def extract_jobID_from_logFile(self):
        '''
        This function reads the logFile and extracts the jobID. It also checks whether logFile is part of array or not.
        LogFile should be of either the two examples shown below:
        @default slurm : slurm-jobID.out
        @custom        : whatever_you_want_to_write_but_no_numbers_directly_before_jobID.out
        @custom  array : whatever_you_want_to_write_but_no_numbers_directly_before_jobID_arrayTaskID.out

        Example of good custom .out file : test_logFile_32287790.out OR test_logFile_32287790_3.out
        Read documentation for further information.
        '''

        slurm_re = re.compile('''slurm''')
        jobID = ""
        if re.match(slurm_re,self.logFile) :
            jobID = self.logFile.split("-")[-1].replace(".out","")
        elif not re.match(slurm_re,self.logFile):
            el = self.logFile.split("_")
            if len(el[-1].replace(".out","")) < 8 and el[-2].isdigit():
                jobID = f"{el[-2]}_{el[-1].replace('.out','')}"
            else :
                jobID = el[-1].replace(".out","")
        return jobID

#print(JobID("slurm-32287790_11.out").extract_jobID_from_logFile())


class jobCheck(object):
    def __init__(self,logFile):
        self.logFile = logFile
        self.jobID = JobID(self.logFile).extract_jobID_from_logFile()

    def jobCheck(self):
        '''Launches sacct command and returns a dictionary with the specific values'''
        downRow = "Account,AllocCPUS,AllocNodes,AssocID,AveCPU,AveCPUFreq,AveDiskRead,AveDiskWrite,AvePages,AveRSS,MaxRSS,AveVMSize,NCPUS,NTasks,Cluster,CPUTime,Elapsed,ExitCode,JobID%100,NodeList,Start,End,State,Submit,JobName,MaxVMSize"  # THese are on the bottom row
        upperRow = "Partition%30,User,Timelimit,WorkDir%10000,ReqMem"  # THese are on the row on top!
        #First we need to check whether jobID is from array or not.

        if len(self.jobID.split("_")) == 1 :
            # Extracting information from sacct from undeRow
            cmd = "sacct --format={info} -j {jobid} --units=M | awk 'NR != 2 && NR != 3' | tr -s ' '".format(info=downRow,jobid=self.jobID)
            jobInfo_under = subprocess.check_output(cmd, shell=True, universal_newlines=True).rstrip().split()
            dico = {jobInfo_under[i]: jobInfo_under[i + (len(jobInfo_under) // 2)] for i in range(0, (len(jobInfo_under) // 2))}

            # Extracting information from sacct from upperRow
            cmd_2 = "sacct --format={info} -j {jobid} --units=M | awk 'NR != 2 && NR != 4' | tr -s ' '".format(info=upperRow, jobid=self.jobID)
            jobInfo_upper = subprocess.check_output(cmd_2, shell=True, universal_newlines=True).rstrip().split()
            dico_2 = {jobInfo_upper[i]: jobInfo_upper[i + (len(jobInfo_upper) // 2)] for i in range(0, (len(jobInfo_upper) // 2))}
            dico.update(dico_2)

            return dico

        else :
            # JobID is an slurm job array.

            # Extracting information from sacct from undeRow
            cmd = "sacct --format={info} -j {jobid} --units=M | awk 'NR != 2 && NR != 3' | sed '2d' | tr -s ' '".format(info=downRow,jobid=self.jobID)
            jobInfo_under = subprocess.check_output(cmd, shell=True, universal_newlines=True).rstrip().split()
            dico = {jobInfo_under[i]: jobInfo_under[i + (len(jobInfo_under) // 2)] for i in range(0, (len(jobInfo_under) // 2))}

            # Extracting information from sacct from upperRow
            cmd_2 = "sacct --format={info} -j {jobid} --units=M | awk 'NR != 2 && NR != 4' | sed '3d' | tr -s ' '".format(info=upperRow, jobid=self.jobID)
            jobInfo_upper = subprocess.check_output(cmd_2, shell=True, universal_newlines=True).rstrip().split()
            dico_2 = {jobInfo_upper[i]: jobInfo_upper[i + (len(jobInfo_upper) // 2)] for i in range(0, (len(jobInfo_upper) // 2))}
            dico.update(dico_2)

            return dico

class Write(object):
    def __init__(self,logFile):
        self.logFile = logFile
        self.jobStats = jobCheck(self.logFile).jobCheck()


    def check_for_command(self):
        '''
        In order to get command, we need to read the first lines of the log FIle and IF executed with wsbatch we should have two lines, one that gives the executed command and another one which is the entire sbatch command submitted.
        '''
        with open(self.logFile, "rt") as f:
            command = f.readline().rstrip()
            if command[:9] == "__WSBATCH" and len(command.split())>1 :
                return command.split('"')[1]
            else:
                return "Job was not submitted using wsbatch OR you used a script and submitted srun script for maybe array therefore no commands can be written. If you want to have command information, please use wsbatch."

    def out(self):

        dico = self.jobStats
        #### CHECKING WHETHER FIRST LINE OF LOG FILE HAS WSBATCH
        command = Write(self.logFile).check_for_command()

        MaxRSS = dico.get('MaxRSS')[:-1]
        AveRSS = dico.get('AveRSS')[:-1]
        ReqMem = dico.get('ReqMem')[:-1]
        MaxVMSize = dico.get('MaxVMSize')[:-1]

        DeltMem = str(round(float(ReqMem[:-1]) - float(MaxRSS), 2))

        lst = []

        lst.append(f"{bcolors.HEADER}SLURM job summary{bcolors.ENDC}")
        lst.append(f"Subject : Job {dico.get('JobID')} in cluster <{dico.get('Cluster')}> Done\n")
        lst.append(
            f"Job <{dico.get('JobName')}> was submitted by user <{dico.get('User')}> in cluster <{dico.get('Cluster')}>.")
        lst.append(
            f"Job was executed on host(s) <{dico.get('NodeList')}>, in queue <{dico.get('Partition')}>, as user <{dico.get('User')}> in cluster <{dico.get('Cluster')}>.")
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
            lst.append(
                f"{bcolors.FAIL}FAILED with Exit code {dico.get('ExitCode')}.{bcolors.ENDC}")

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
        lst.append("\n")

        return lst



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


B_parser=subparsers.add_parser("write",help="Writes the job summary into a new file.")
B_IO = B_parser.add_argument_group('Main arguments')
B_IO.add_argument("-log","--logFile",
                    type=str,
                    nargs=1,
                    dest='logFile',
                    action="store",
                    default=None,
                    help="log file name")
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

#D_parser=subparsers.add_parser("quick_check",help="Checks only for exit status of slurm job. Outputs if successfully completed or failed.")
#D_IO = D_parser.add_argument_group('Main arguments')
#D_IO.add_argument("-log","--logFile",
#                    type=str,
#                    nargs=1,
#                    dest='logFile',
#                    action="store",
#                    default=None,
#                    help="log file name")

args = parser.parse_args()

if __name__ == "__main__":
    if args.cmd == "screen":
        if args.logFile == None :
            raise Exception("Please specify a proper logFile name")
        else :
            info = Write(args.logFile[0]).out()
            for i in info:
                print("".join(i))

    if args.cmd == "write" :
        if args.logFile == None or args.output == None:
            raise Exception("Please specify either a proper logFile name or proper jobID and and output file.")
        else :
            g = open(args.output,"w")
            info = Write(args.logFile[0]).out()
            for i in info:
                g.write("".join(i) + "\n")

    if args.cmd == "append":
        if args.logFile == None:
            raise Exception("Please specify either a proper logFile name.")
        else :
            info = Write(args.logFile[0]).out()
            info.append(f"\nThe output if any follows below:")
            info.append("\n")
            prepend_multiple_lines(args.logFile[0], info)






















