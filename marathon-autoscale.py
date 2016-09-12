__author__ = 'tkraus'

import sys
import requests
import json
import math
import time
import os

marathon_host = os.getenv('MARATHON_HOST', 'marathon.mesos')
marathon_app = os.getenv('MARATHON_APP', '___none___')
max_mem_percent = int(os.getenv('MAX_MEM_PERCENT', 80))
max_cpu_time = int(os.getenv('MAX_CPU_TIME', 80))
min_cpu_time = int(os.getenv('MIN_CPU_TIME', 50))
trigger_mode = os.getenv('TRIGGER', 'or')
autoscale_multiplier = float(os.getenv('SCALE_MULT', 1.5))
max_instances = int(os.getenv('MAX_INSTANCES', -1))
min_instances = int(os.getenv('MIN_INSTANCES', 1))

if (marathon_app == '___none___'):
    print ('Must provide MARATHON_APP var')
    exit()

if (max_instances < 1):
    print ('MAX_INSTANCES must be > 0')
    exit()

class Marathon(object):

    def __init__(self, marathon_host):
        self.name = marathon_host
        self.uri=("http://"+marathon_host+":8080")

    def get_all_apps(self):
        response = requests.get(self.uri + '/v2/apps').json()
        if response['apps'] ==[]:
            print ("No Apps found on Marathon")
            sys.exit(1)
        else:
            apps=[]
            for i in response['apps']:
                appid = i['id'].strip('/')
                apps.append(appid)
            print ("Found the following App LIST on Marathon =", apps)
            self.apps = apps # TODO: declare self.apps = [] on top and delete this line, leave the apps.append(appid)
            return apps

    def get_app_details(self, marathon_app):
        response = requests.get(self.uri + '/v2/apps/'+ marathon_app).json()
        if (response['app']['tasks'] ==[]):
            print ('No task data on Marathon for App !', marathon_app)
        else:
            app_instances = response['app']['instances']
            self.appinstances = app_instances
            print(marathon_app, "has", self.appinstances, "deployed instances")
            app_task_dict={}
            for i in response['app']['tasks']:
                taskid = i['id']
                hostid = i['host']
                print ('DEBUG - taskId=', taskid +' running on '+hostid)
                app_task_dict[str(taskid)] = str(hostid)
            return app_task_dict

    def scale_app(self,marathon_app,target_instances):
        #target_instances_float=self.appinstances * autoscale_multiplier
        #target_instances=math.ceil(target_instances_float)
        if (target_instances > max_instances):
            print("Reached the set maximum instances of", max_instances)
            target_instances=max_instances
        elif (target_instances < min_instances):
            print("Won't scale below %s instances", min_instances)
            target_instances=min_instances

        if (target_instances == self.appinstances):
            print('Not scaling as we are at current target')
            return

        data ={'instances': target_instances}
        json_data=json.dumps(data)
        headers = {'Content-type': 'application/json'}
        response=requests.put(self.uri + '/v2/apps/'+ marathon_app,json_data,headers=headers)
        print ('Scale_app return status code =', response.status_code)

def get_task_agentstatistics(task, host):
    # Get the performance Metrics for all the tasks for the Marathon App specified
    # by connecting to the Mesos Agent and then making a REST call against Mesos statistics
    # Return to Statistics for the specific task for the marathon_app
    response = requests.get('http://'+host + ':5051/monitor/statistics.json').json()
    #print ('DEBUG -- Getting Mesos Metrics for Mesos Agent =',host)
    for i in response:
        executor_id = i['executor_id']
        #print("DEBUG -- Printing each Executor ID ", executor_id)
        if (executor_id == task):
            task_stats = i['statistics']
            # print ('****Specific stats for task',executor_id,'=',task_stats)
            return task_stats

def timer():
    sleep_for=10
    print("Successfully completed a cycle, sleeping for %d seconds ..." % (sleep_for))
    time.sleep(sleep_for)
    return

if __name__ == "__main__":
    print ("This application tested with Python3 only")
    running=1
    last_cpu_time=-1
    sample_time=-1
    last_sample_time=-1
    last_instances=-1
    was_scaled=False
    while running == 1:
        # Initialize the Marathon object
        aws_marathon = Marathon(marathon_host)
        # Call get_all_apps method for new object created from aws_marathon class and return all apps
        marathon_apps = aws_marathon.get_all_apps()
        print ("The following apps exist in Marathon...", marathon_apps)
        # Quick sanity check to test for apps existence in MArathon.
        if (marathon_app in marathon_apps):
            print ("  Found your Marathon App=", marathon_app)
        else:
            print ("  Could not find your App =", marathon_app)
            sys.exit(1)
        # Return a dictionary comprised of the target app taskId and hostId.
        app_task_dict = aws_marathon.get_app_details(marathon_app)
        print ("    Marathon  App 'tasks' for", marathon_app, "are=", app_task_dict)

        app_cpu_values = []
        app_mem_values = []
        app_cpu_times = []
        for task,host in app_task_dict.items():
            # Compute CPU usage
            task_stats = get_task_agentstatistics(task, host)
            cpus_system_time_secs0 = float(task_stats['cpus_system_time_secs'])
            cpus_user_time_secs0 = float(task_stats['cpus_user_time_secs'])
            timestamp0 = float(task_stats['timestamp'])

            sample_time=timestamp0
            cpus_time_total0 = cpus_system_time_secs0 + cpus_user_time_secs0

            # RAM usage
            mem_rss_bytes = int(task_stats['mem_rss_bytes'])
            print ("task", task, "mem_rss_bytes=", mem_rss_bytes)
            mem_limit_bytes = int(task_stats['mem_limit_bytes'])
            print ("task", task, "mem_limit_bytes=", mem_limit_bytes)
            mem_utilization = 100 * (float(mem_rss_bytes) / float(mem_limit_bytes))
            print ("task", task, "mem Utilization=", mem_utilization)
            print()

            app_cpu_times.append(cpus_time_total0)
            app_mem_values.append(mem_utilization)

        sum_cpu_time = sum(app_cpu_times)

        print ('Total CPU time=', sum_cpu_time)

        if (last_cpu_time == -1):
            print ('First iteration, saving CPU usage for next')
            last_cpu_time = sum_cpu_time
            last_sample_time = sample_time
            last_instances = aws_marathon.appinstances
            timer()
            continue

        delta_cpu_time = sum_cpu_time - last_cpu_time
        timestamp_delta = sample_time - last_sample_time
        delta_per_task = delta_cpu_time / aws_marathon.appinstances
        app_avg_cpu = float(delta_per_task / timestamp_delta) * 100

        print ('TimeLast=%d, TimeCurr=%d, DeltaPerTask=%d, Percentage=%d, InstLast=%d, InstCurr=%d' % (last_cpu_time, sum_cpu_time, delta_per_task, app_avg_cpu, last_instances, aws_marathon.appinstances))

        last_sample_time = sample_time
        last_cpu_time = sum_cpu_time
        last_instances = aws_marathon.appinstances
        print ('Current Average  CPU Time for app', marathon_app, '=', app_avg_cpu)
        app_avg_mem=(sum(app_mem_values) / len(app_mem_values))
        print ('Current Average Mem Utilization for app', marathon_app,'=', app_avg_mem)

        print('\n')
        #target_instances_float=aws_marathon.appinstances * autoscale_multiplier
        #target_up=math.ceil(target_instances_float)
        target_up = aws_marathon.appinstances+1
        target_down = aws_marathon.appinstances-1
        target=aws_marathon.appinstances
        if (trigger_mode == "and"):
            if (app_avg_cpu > max_cpu_time) and (app_avg_mem > max_mem_percent):
                target=target_up
            elif (app_avg_cpu < min_cpu_time):
                target_target_down
        elif (trigger_mode == "or"):
            if (app_avg_cpu > max_cpu_time) or (app_avg_mem > max_mem_percent):
                target=target_up
            elif (app_avg_cpu < min_cpu_time):
                target=target_down

        target=max(min(target, max_instances), min_instances)

        if(was_scaled):
            print('Waiting another cycle to scale')
            was_scaled=False
            target = aws_marathon.appinstances
        else:
            if (target != aws_marathon.appinstances):
                print("Scaling to ", target)
                aws_marathon.scale_app(marathon_app, target)
                was_scaled=True
            else:
                print ('Not scaling - target same as current')

        timer()
