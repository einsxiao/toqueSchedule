#!/usr/bin/env python
#######################
###  Auth: Hu, Xiao
###  Date: 3.29.2017
#######################
from evawiz_basic import *
import traceback
import os
import sys
import commands
import time
import re

def log(name,content,log_dir=None):
    hour_str =  time.strftime('%Y%m%d_%H:%M ',time.localtime(time.time()) )
    content =hour_str + content + "\n"
    home_dir = os.getenv("HOME")
    os.system("if ! [ -d ~/logs/ ]; then mkdir ~/logs; fi ")
    log_file = home_dir+"/logs/"+name
    if log_dir:
        os.system("if ! [ -d ~/logs/%s ]; then mkdir ~/logs/%s; fi "%(log_dir,log_dir))
        log_file = home_dir+"/logs/"+log_dir+"/"+name
    if not os.path.exists( log_file ):
        file_content_set(log_file,content)
    else:
        file_content_append(log_file,content)
        pass
    pass

def dpl(ll,name="",N=7):
    if type(ll) == dict:
        if name: print "%s:"%name
        i=0;
        for item in ll:
            print i,':',item,ll[item]
            i=i+1
            if i>N: break
        print '...'
        print '...'
        return
    if type(ll) == list:
        if name: print "%s:"%name
        i=0;
        for item in ll:
            print i,':',item
            i=i+1
            if i>N: break
        print '...'
        print '...'
        return
    print "%s: "%(name),ll

def file_content_get(filepath):
    filecontent = ''
    with open(filepath,'rb') as fp:
        while True:
            blk = fp.read(32768) # 32kb per block
            if not blk: break
            filecontent += blk
    return filecontent

def file_content_set(filepath,filecontent=''):
    with open(filepath,'wb') as fp:
        fp.write(filecontent)

def start_with(line,word):
    if line[0:len(word)]==word:
        return True
    return False

def trim_comment(line):
    pos = line.find('#')
    if pos<0:
        return line.strip()
    else:
        return line[:pos].strip()
def strip_str(string):
    return string.strip()

def strip_list(ll):
    return map(strip_str,ll)
def is_int(s):
    try:
        int(s)
        return True
    except Exception,e:
        return False
    pass

class Schedule:
    sypermit = None
    userspermit = None
    usersmail = None

    jobsstate = None
    usersstate = None
    nodesstate = None
    queuesstate = None
    queuejobs = None

    completedjobs = None #qdel -p jobid to remove completed status

    usagePercent= 0
    permRate = 1
    
    def __init__(self):
        #initailize at startup
        self.build_system_perm()
        #dpl(self.syspermit,"permission settings of the system")
        #dpl(self.userspermit,"permission settings for users")
        #dpl(self.usersmail,"user mail list")

    def update_state(self):
        usersstate = None
        nodesstate = None
        queuesstate = None
        queuejobs = None 

        #need to update every time schedule
        self.build_system_perm()
        self.build_job_state()
        self.build_user_state()
        self.build_node_state()
        self.build_queue_state()
        #dpl(self.jobsstate,"job state list")
        #dpl(self.usersstate,"user state list")
        #dpl(self.nodesstate,"node state list")
        #dpl(self.queuesstate,"queue state list")


    def build_system_perm(self):
        self.syspermit = {}
        self.userspermit = {}
        self.usersmail = {}

        #default values
        self.syspermit['classes'] = ('high','middle','normal','low')

        permlines = file_content_get(os.getenv("HOME")+'/evawiz/torqueSchedule/perm.config').split('\n')
        lineN = len(permlines)
        next_line = trim_comment(permlines[0]).strip()
        for i in range(lineN):
            line = next_line
            if i < lineN -1 :
                next_line = trim_comment(permlines[i]).strip()
                if not line: continue #nothing in line
                if line[-1] != ';' and next_line.find('=')<0: #two lines are connected
                    next_line = line+next_line
                    continue
            if not line: continue
            if line[-1] == ';': line=line[:-1]
            self.syspermit['user_home']=('/home/',)
            if re.match('^user_home\ *=',line):
                self.syspermit['user_home']=strip_list( line.split('=')[1].split(',') )
                continue

            #queue profile
            if re.match('^queue_default_priority\ *=',line):
                self.syspermit['queue_default_priority'] = line.split('=')[1].strip()
                continue

            if re.match('^queue_priority\ *=',line):
                self.syspermit['queue_priority'] = {}
                for item in  line.split('=')[1].split(',') :
                    item = item.split(':')
                    self.syspermit['queue_priority'][ item[0].strip() ] = item[1].strip()
                    pass
                continue
            if re.match('^gpu_queues\ *=',line):
                self.syspermit['gpu_queues'] = strip_list( line.split('=')[1].split(',') )
                continue;

            #user class profile
            if re.match('^classes\ *=',line):
                self.syspermit['classes'] = strip_list( line.split('=')[1].split(',') )
                continue;

            if re.match('^default_class\ *=',line):
                self.syspermit['default_class'] = line.split('=')[1].strip()
                continue

            if re.match('^\w+_class\ *=',line):
                line = line.split('=')
                classname = line[0].strip()
                #print line,classname
                self.syspermit[classname] = strip_list( line[1].split(',') )
                continue

            if re.match('^\w+_class_perm\ *=',line):
                line = line.split('=')
                classpermname = line[0].strip()
                self.classperm = self.syspermit[classpermname]={}
                for item in line[1].split(','):
                    item = strip_list( item.split(':') )
                    self.classperm[ item[0] ] = item[1] 
                continue

            if re.match('^\w+_class_percentage\ *=',line):
                line = line.split('=')
                classpermname = line[0].strip()
                self.syspermit[classpermname]=line[1].strip()
                continue

            if re.match('^user_perm\ *=',line):
                line = line.split('=')[1].split('/')
                username = line[0].strip()
                userpermit = self.userspermit[username] = {}
                for item in line[1].split(','):
                    item = strip_list( item.split(':') )
                    userpermit[ item[0] ] = item[1] 
                continue

            if re.match('^user_mail\ *=',line):
                line = line.split('=')[1].split('/')
                username = line[0].strip()
                self.usersmail[username] = strip_list( line[1].split(',') )
                continue
               
            #irelevent line
            continue

    def user_perm(self,username=''):
        if username:
            if username in self.userspermit:
                return self.userspermit[username]
            for classname in self.syspermit['classes']:
                if username in self.syspermit[classname+"_class"]:
                    return self.syspermit[classname+"_class_perm"];
        classname = self.syspermit['default_class'];
        return self.syspermit[classname+"_perm"];

    def build_job_state(self):
        self.jobsstate={}
        self.queuejobs = []
        self.completedjobs = []

        (status,qstate)=commands.getstatusoutput('qstat -f')
        if status != 0: raise Exception('Error while execute system command "qstat"')
        qstate = qstate.split('\n')
        lineN=len(qstate)
        if lineN== 0: return
        next_line = qstate[0]
        for i in range(lineN-1):
            line = next_line
            if i < lineN-2: next_line = qstate[i+1]
            if start_with(line,'Job '):
                jobid = line.split()[2].split('.')[0]
                self.jobsstate[jobid] = {}
                jobstate = self.jobsstate[jobid]
                jobstate['id']=jobid
                #jobstate['occupied_ppns']=0
                #jobstate['occupied_nodes']=0
                continue
            if not start_with(next_line,'    ') and not start_with(next_line,'Job '):
                next_line = line+next_line.strip()
                continue
                                                                   
            #print 'deal>>>%s<<<'%line
            line = line.strip()
            #get sub info
            if start_with(line,'Job_Owner ='):
                jobstate['user'] = line.split()[2].split('@')[0]
                continue
            if start_with(line,'job_state ='):
                job_state = jobstate['state'] = line.split()[2]
                if job_state == 'Q':
                    self.queuejobs.append(jobid)
                    pass
                if job_state == 'C':
                    self.completedjobs.append(jobid)
                    pass
                continue
            if start_with(line,'total_runtime ='):
                jobstate['total_runtime'] = line.split()[2]
                continue
            if start_with(line,'resources_used.cput ='):
                jobstate['resources_used.cputime'] = line.split()[2]
                continue
            if start_with(line,'resources_used.mem ='):
                jobstate['resources_used.memory'] = line.split()[2]
                continue
            if start_with(line,'start_time ='):
                jobstate['start_time'] = line.split('=')[1]
                continue
            if start_with(line,'comp_time ='):
                jobstate['comp_time'] = line.split('=')[1]
                continue

            if start_with(line,'queue ='):
                jobstate['queue'] = line.split()[2]
                continue
            if start_with(line,'exec_host ='):
                jobstate['exec_host'] = line.split()[2]
                continue
            if start_with(line,'exec_gpus ='):
                jobstate['exec_gpus'] = line.split()[2]
                continue
            if start_with(line,'Resource_List.nodect ='):
                jobstate['nodect'] = line.split()[2]
                continue
            if start_with(line,'Resource_List.neednodes ='):
                neednodesstr = line.split()[2]
                need_nodes = 0
                need_ppns = 0
                need_gpus = 0
                jobstate['res_request'] = []
                for items in neednodesstr.split("+"):
                    items = items.split(":")
                    res_request = {}
                    res_request['type']='normal'
                    res_request['nodes']=1
                    res_request['ppn']=1
                    res_request['gpus']=0
                    if ( len( items ) >= 1 ): 
                        if is_int( items[0] ):
                            res_request['type'] = 'normal'
                            res_request['nodes'] = int( items[0] )
                            pass
                        else:
                            res_request['type'] = 'specific'
                            res_request['nodes'] = items[0]
                    else: continue
                    while ( len( items ) >= 2 ):
                        items[1] = items[1].split('=')
                        if ( len( items[1] ) != 2 ): continue
                        if ( not is_int(items[1][1]) ): continue
                        res_request[ items[1][0] ] = int( items[1][1] )
                        del items[1]
                        pass
                    if ( res_request['type']=='normal' ):
                        need_nodes += res_request['nodes']
                        need_ppns += res_request['nodes'] * res_request['ppn']
                        need_gpus += res_request['nodes'] * res_request['gpus']
                    else:
                        need_nodes += 1
                        need_ppns += res_request['ppn']
                        need_gpus += res_request['gpus']
                        pass
                    jobstate['res_request'].append( res_request )
                    pass
                jobstate['need_nodes'] = need_nodes
                jobstate['need_ppns'] = need_ppns
                jobstate['need_gpus'] = need_gpus

            if start_with(line,'Resource_List.walltime ='):
                jobstate['walltime'] = line.split()[2]
                continue
            if start_with(line,'Resource_List.'):
                line = line.split('Resource_List.')[1].split(':')
                for item in line:
                    item = item.split('=')
                    if ( len(item) !=2 ):continue
                    jobstate[item[0]] = item[1]
                continue

            if start_with(line,'submit_host ='):
                line = line.split('submit_host =')[1]
                jobstate['submit_host'] = line
                #calculate occupied ppns and occupied nodes; waltime is the line below nodect and nodes
                # nodes = jobstate.get('nodect')
                # if nodes: nodes = int(nodes)
                # ppns = jobstate.get('ppn')
                # if ppns: ppns = int(ppns)
                # jobstate['need_nodes'] = nodes 
                # if not ppns:
                #     jobstate['need_ppns'] = nodes
                #     jobstate['ppn'] = 1
                # else:
                #     jobstate['need_ppns'] = nodes*int(ppns)
                #     gpus = jobstate.get('gpus')
                #     if gpus: gpus = int( gpus )
                # if gpus:
                #     jobstate['need_gpus'] = nodes*int(gpus)
                #     jobstate['gpu'] = int(gpus)
                # else:
                #     jobstate['need_gpus'] = 0
                #     jobstate['gpu'] = 0
                continue
            #unrelevent lines
            #continue
            if start_with(line,''):
                jobstate[''] = line.split()[2]
                continue
            pass
        pass

    def build_user_state(self):
        self.usersstate = {}

        for jobid in self.jobsstate:
            job = self.jobsstate[jobid]
            #print job;
            user = job['user']
            userstate = self.usersstate.get(user)
            if not userstate:
                self.usersstate[user]={}
                userstate = self.usersstate[user]
                userstate['run_jobs'] = 0
                userstate['queue_jobs'] = 0
                userstate['occupied_hosts'] = {}
                userstate['occupied_ppns'] = 0
                userstate['occupied_nodes'] = 0
                userstate['occupied_gpus'] = 0
                pass
            jobstate = job['state']
            if jobstate == 'R':
                userstate['run_jobs'] += 1
                #calculate the occupied_ppns
                hosts = job['exec_host'].split('+')
                for host in hosts: userstate['occupied_hosts'][host] = True
                userstate['occupied_nodes'] = len(userstate['occupied_hosts'])
                userstate['occupied_ppns'] += len( hosts )
                userstate['occupied_gpus'] += job['need_gpus']
                pass
            elif jobstate == 'Q':
                userstate['queue_jobs'] += 1
                pass
            pass
        #print self.usersstate
        pass

        

        #>>>occupied_nodes<<< info will be appended within procedure build_node_state
            

    def build_node_state(self):
        total_ppns = 0
        occupied_ppns = 0
        self.nodesstate = {}
        self.nodesstate['total_ppns'] = 0
        self.nodesstate['total_gpus'] = 0
        self.nodesstate['occupied_ppns'] = 0
        self.nodesstate['available_ppns'] = 0
        self.nodesstate['occupied_gpus'] = 0
        self.nodesstate['available_gpus'] = 0
        self.nodesstate['free_ppns_nodes'] = []
        self.nodesstate['free_gpus_nodes'] = []
        (status,output) = commands.getstatusoutput('pbsnodes ')
        lines = output.split('\n')
        lineN = len(lines)
        for i in range(lineN):
            line = lines[i]
            #print "deal>>>%s<<<"%line
            if not start_with(line,'     '):
                if not line.strip(): continue
                nodename = line.strip()
                self.nodesstate[nodename] = {}
                nodestate = self.nodesstate[nodename]
                occupied_ppns = nodestate['occupied_ppns'] = 0
                occupied_gpus = nodestate['occupied_gpus'] = 0
                avaliable_ppns = nodestate['avaliable_ppns'] = 0
                avaliable_gpus= nodestate['avaliable_gpus'] = 0
                np = 0
                gpus = 0
                continue
            #items 
            line = line.strip()
            if start_with(line,'state ='):
                state = nodestate['state'] = line.split()[2]
                continue
            if start_with(line,'np ='):
                np = nodestate['np'] = line.split()[2]
                np = int(np)
                self.nodesstate['total_ppns'] += np
                continue
            if start_with(line,'properties ='):
                nodestate['properties'] = line.split()[2].split(',')
                continue
            if start_with(line,'jobs ='):
                jobs = line.split()[2].split(',')
                occupied_ppns = nodestate['occupied_ppns'] = len(jobs)
                run_users={};
                for job in jobs:
                    jobid = job.split('/')[1].split('.')[0]
                    #print jobid
                    jobstate = self.jobsstate[jobid]
                    #print jobstate
                    jobowner = jobstate['user']
                    run_users[jobowner] = 1;
                    #tell if gpu used
                    job_exec_gpus = jobstate.get('exec_gpus')
                    if job_exec_gpus:
                        for gpu in  job_exec_gpus.split('+'):
                            host = gpu.split('-')[0]
                            #print nodename,host
                            if nodename == host:
                                occupied_gpus +=1
                                userstate['occupied_gpus']+=1
                    continue 
                    #common
                    #jobstate['occupied_ppns'] += 1
                    self.usersstate[jobowner]['occupied_ppns'] += 1
                #print "try to increase number of occupied_nodes of %s"%(jobstate['user'])
                for (user,value) in run_users.items():
                    self.usersstate[user]['occupied_nodes']+=1;

                continue
            if start_with(line,'gpus ='):
                gpus = int(line.split()[2])
                self.nodesstate['total_gpus'] += 1
                continue
            if start_with(line,'properties ='):
                nodestate['properties'] =  strip_list( line.split('=')[1].split(',') )
                continue
            #sum at the end of one node
            if i == lineN-1 or not start_with(lines[i+1],'     ') :
                if state == 'free' or state == 'job-exclusive':
                    freenp = int(np-occupied_ppns)
                    freegpus = int(gpus - occupied_gpus)
                    nodestate['occupied_ppns'] = int(occupied_ppns)
                    nodestate['avaliable_ppns'] = int(freenp)
                    nodestate['occupied_gpus'] = int(occupied_gpus)
                    nodestate['avaliable_gpus'] = int(freegpus)
                    self.nodesstate['available_ppns'] += int(freenp)
                    self.nodesstate['available_gpus'] += int(freegpus)
                    if freenp != 0: self.nodesstate['free_ppns_nodes'].append( (freenp,nodename) )
                    if freegpus != 0 and freenp != 0: self.nodesstate['free_gpus_nodes'].append( (freegpus,nodename) )
                    #print occupied_ppns
                    self.nodesstate['occupied_ppns'] += occupied_ppns
                    self.nodesstate['occupied_gpus'] += occupied_gpus
                continue
            #other ignored lines
            continue
        self.revise_node_state()
        self.usagePercent = float( self.nodesstate['occupied_ppns'] + self.nodesstate['occupied_gpus']*16 ) / (self.nodesstate['total_ppns'] + self.nodesstate['total_gpus'] )
        self.permRate = 0.8/(self.usagePercent+0.1);
        self.permRate = 1;
        print "#############################################################"
        print "usage = %s, perm rate = %s"%(self.usagePercent,self.permRate )
        print "#############################################################"

        pass

    def revise_node_state(self):
        for jobid in self.jobsstate:
            job = self.jobsstate[jobid]
            jobstate = job['state']
            # only consider the R state
            #consider only gpu jobs
            if jobstate != "R": continue 
            # if ( jobid != '100477' ):
            #     continue
            ## get which host is the job running
            #print jobid, job
            hosts = job['exec_host'].split('+')
            host_list = []
            for host in hosts:
                host = host.split('/')[0]
                if not host in host_list:
                    host_list.append( host )
                    pass
                pass
            #print host_list
            for host in host_list:
                nodestate = self.nodesstate[host]
                nodestate['occupied_gpus'] += job['need_gpus']
                nodestate['avaliable_gpus'] -= job['need_gpus']
                #print self.nodesstate[host]
                pass
            pass
        pass

    def build_queue_state(self):
        self.queuesstate = {}
        (status,lines) = commands.getstatusoutput('qstat -Q')
        lines = lines.split('\n')[2:]
        for line in lines:
            line = line.split()
            name  = line[0]
            queuestate = self.queuesstate[name] = {'name':name}
            enabled = queuestate['enabled'] = line[3]
        pass

    def log_completed_jobs(self):
        for jobid in self.completedjobs:
            print "##############################"
            print 'try to check completed job %s'%jobid
            jobstate = self.jobsstate[jobid]
            #log job info
            date_str =  time.strftime('%Y%m%d',time.localtime(time.time()) )
            hour_str =  time.strftime('%Y%m%d_%H:%M',time.localtime(time.time()) )
            state_str = "accounting job(%s) user(%s) ppns(%s) gpus(%s) "%( jobstate['id'],jobstate['user'],jobstate['need_ppns'],jobstate['need_gpus'], )
            normal = True
            if 'total_runtime' in jobstate:
                state_str += "cputime(%s) "%(jobstate['total_runtime'],)
            else:
                normal = False
                pass
            if 'resources_used.cputime' in jobstate:
                state_str += "cputime(%s) "%(jobstate['resources_used.cputime'],)
            else:
                normal = False
                pass
            if 'start_time' in jobstate:
                state_str += "starttime(%s) "%(jobstate['start_time'],)
            else:
                normal = False
                pass
            if 'comp_time' in jobstate:
                state_str += "completetime(%s) "%(jobstate['comp_time'],)
            else:
                normal = False
                pass

            if normal:
                log( date_str,state_str, "job_log")
            else:
                log( date_str+"_abnormal",state_str, "job_log")

            cmd = "qdel -p %s"%(jobstate['id'])
            print "try execute %s"%(cmd,)
            os.system(cmd)
            pass

        pass

    def run_perm_jobs(self):
        self.update_state()
        free_ppns_nodes = sorted(self.nodesstate['free_ppns_nodes'])
        #dpl(free_ppns_nodes,"list of node with free ppns")
        free_gpus_nodes = sorted(self.nodesstate['free_gpus_nodes'])
        #dpl(free_gpus_nodes,"list of node with free gpus")
        #schedule from high priority to low
        #cur_priority = self.syspermit['queue_priority']
        #print self.queuejobs
        rate = self.permRate
        for jobid in self.queuejobs:
            print "##############################"
            print 'try to check job %s'%jobid
            jobstate = self.jobsstate[jobid]
            print "deal job :",jobstate
            job_owner = jobstate["user"]
            userperm = self.user_perm(job_owner);
            userstate = self.usersstate[job_owner];
            print "user perm: ",userperm;
            print "user state:",userstate;
            print "check restrictions"
            if ( int(userstate['occupied_ppns'])+int(jobstate['need_ppns']) > int(userperm['max_ppns']) )*rate:
                print "exceed allowed ppns" 
                continue
            if ( int(userstate['occupied_nodes'])+int(jobstate['need_nodes']) > int(userperm['max_nodes']) )*rate:
                print "exceed allowed nodes"
                continue
            if ( int(userstate['occupied_gpus'])+int(jobstate['need_gpus']) > int(userperm['max_gpus'] ) )*rate:
                print "exceed allowed gpus"
                continue
            print "All restriction(user side) meet."
            ###################
            #find out hosts to run
            total_request_nodes = jobstate['need_nodes']
            total_request_ppns = jobstate['need_ppns']
            total_request_gpus = jobstate['need_gpus']
            # print "total_request_nodes =" ,total_request_nodes
            # print "total_request_gpus =" ,total_request_gpus
            # print "total_request_ppns =" ,total_request_ppns
            nodelist = []
            gpunodelist = []
            for request in jobstate['res_request']:
                # type 1 normal request
                if ( request['type'] == 'normal' ):
                    # print " dealing normal request"
                    request_nodes = request['nodes']
                    request_ppns = request['ppn'] * request_nodes
                    request_gpus = request['gpus'] * request_nodes
                    if request_gpus == 0:
                        test_nodes = free_ppns_nodes
                    else:
                        test_nodes = free_gpus_nodes
                        pass
                    for node in test_nodes:
                        nodename = node[1];
                        # print "test node ",nodename
                        nodestate = self.nodesstate[ nodename ]
                        if not (jobstate['queue'] in nodestate['properties']) :continue
                        # print "check %s with state %s"%(node,nodestate)
                        if int(nodestate['avaliable_gpus']) < int(request['gpus']): continue
                        if int(nodestate['avaliable_ppns']) < int(request['ppn']): continue
                        if int(nodestate['avaliable_ppns']) - int(nodestate['avaliable_gpus']) + int(request['gpus']) < int(request['ppn']): continue
                        # print " check with requirement met"
                        request_nodes -= 1;
                        request_gpus -= request['gpus'];
                        request_ppns -= request['ppn'];
                        total_request_nodes -= 1; 
                        total_request_gpus -= request['gpus']
                        total_request_ppns -= request['ppn']
                        for i in range(0, int(request['ppn']) ):
                            nodelist.append(nodename);
                            pass
                        for i in range(0, int(request['gpus'])):
                            gpunodelist.append(nodename);
                            pass
                        if request_nodes == 0: break;
                        pass
                    if ( request_nodes != 0 ): break;
                    # print "find resources for request"
                    pass
                # type 2 specifict request
                if ( request['type'] == 'specific' ):
                    # print " dealing specific request"
                    if not self.nodesstate.has_key( request['nodes'] ): raise Exception("node %s unknow"%(request['nodes'],))
                    nodestate = self.nodesstate[ request['nodes'] ]
                    if int(nodestate['avaliable_gpus']) < int(request['gpus']): continue
                    if int(nodestate['avaliable_ppns']) < int(request['ppn']): continue
                    if int(nodestate['avaliable_ppns']) - int(nodestate['avaliable_gpus']) + int(request['gpus']) < int(request['ppn']): continue
                    # print " check requirement met"
                    total_request_nodes -=1;
                    total_request_ppns -= request['ppn'];
                    total_request_gpus -= request['gpus'];
                    for i in range(0, request['ppn']):
                        nodelist.append( request['nodes'] )
                        pass
                    for i in range(0, request['gpus'] ):
                        gpunodelist.append( request['nodes'] )
                        pass
                    pass
                pass
            # print "total_request_nodes =" ,total_request_nodes
            # print "total_request_gpus =" ,total_request_gpus
            # print "total_request_ppns =" ,total_request_ppns
            if ( total_request_nodes != 0 ) :
                print "!!!!!!!!resources is not enough."
                continue
            print "success to find resources"
            print "node list = ",nodelist
            #construct the nodelist
            if (len(nodelist) == 0 ):
                print "Error: no node list found."
                continue
            cmdstr= "qrun %s -H %s"%(jobstate['id'],nodelist[0])
            for i in range(1,len(nodelist) ):
                cmdstr+="+%s"%(nodelist[i])
                continue
            print "!!!!!!!!try run cmd %s"%(cmdstr,)
            if ( os.system(cmdstr) == 0 ):
                #log job info
                date_str =  time.strftime('%Y%m%d',time.localtime(time.time()) )
                hour_str =  time.strftime('%Y%m%d_%H:%M',time.localtime(time.time()) )
                log( date_str,
                     "starting job(%s) user(%s) time(%s) ppns(%s) gpus(%s)"%(jobstate['id'],jobstate['user'],hour_str,jobstate['need_ppns'],jobstate['need_gpus'],),
                     "job_log")
                # revise jobstate and nodestate
                for i in nodelist:
                    nodesstate[i]['occupied_ppns'] += 1
                    nodesstate[i]['avaliable_ppns'] -= 1
                    pass
                for i in gpunodelist:
                    nodesstate[i]['occupied_gpus'] += 1
                    nodesstate[i]['avaliable_gpus'] -= 1
                    pass
                continue
            # offline these nodes of error occured
            log('qrun_error',"qrun error when executing %s"%(cmdstr,))
            last_node = ""
            for node in nodelist:
                if nodelist[i] != last_node:
                    os.system("pbsnodes -o %s"%(node,))
                    log('qrun_error'," offline node %s"%(node,))
                    last_node == node
                    pass
                pass
            break
        pass

    def run_percentage_jobs(self):
        pass

    def run_cpujobs_on_jobs(self):
        pass

    def run_jobs(self):
        self.run_perm_jobs()
        self.run_percentage_jobs()
        pass

    def run_forever(self):
        while ( True ):
            try:
                self.run_jobs()
                self.log_completed_jobs()
                time.sleep(2)
                pass
            except Exception, e:
                log("schedule_exit_error","schedule exist with error")
                print "schedule_exit_error","schedule exist with error"
                log("schedule_exit_error",traceback.format_exc() )
                print "schedule_exit_error:",traceback.format_exc()
                time.sleep(10)
                pass
            pass
        pass
            

        

