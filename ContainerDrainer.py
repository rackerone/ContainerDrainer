#!/usr/bin/env python
# -*- coding: utf-8 -*-
#Copyright 2014 Aaron Smith
#Container Drainer Maintainer = aaron.smith@rackspace.com

#Use this script to destroy/delete a container.  The maximum pool size will vary
#between various servers/hardware.

#===   DEPENDENCIES   ==> tested on: [Ubuntu 14.04 LTS]
#Tested on pyrax==1.9.0 and Python 2.7
#setup for ContainerDrainer.py
"""
$ apt-get update
$ apt-get install gcc
$ apt-get install libevent-dev python-all-dev
$ apt-get install python-gevent
$ pip install six --upgrade

--->If you had a previous installation of gevent on Ubuntu using 'pip', then uninstall it after/before the
    apt-get installation of gevent.
$ pip uninstall gevent
"""

#IMPORTANT: we have to import gevent modules and then "monkey.patch_all()" BEFORE we import the rest of the modules
from gevent import monkey
from gevent.pool import Pool
from gevent import Timeout
#I have to supply "thread=False, socket=False" to .patch_all to prevent errors when/if creating MP manager queueu
monkey.patch_all()
import pyrax
import os
import sys
import time
import logging
import argparse
import multiprocessing


def confirm(username, apikey, myregion, mycontainer, public, runonce, loglevel, concurrency):
    """Display provided information for verification and require user action to continue"""
    print ("=" * 20)
    print("Username: %s" % username)
    print("API Key: %s" % apikey)
    print("Region: %s" % myregion)
    print("Target Container: %s" % mycontainer)
    print("Use ServiceNet: %s" % public)
    print("Run Once: %s" % runonce)
    print("Log Level: %s" % loglevel)
    print("Concurrency Level: %s" % concurrency)
    print ("=" * 20)

def delete_object(obj):
    """This function is the worker that will delete obj from the object list"""
    with Timeout(5, False):
        try:
            cont.delete_object(obj)
            log.info("successfully deleted [%s]" % obj)
        except Exception as e:
            #We will maintain a list of errors on the screen
            log.error("Failed to delete %s" % obj)
            #log.debug("    %s" % e)

def run():
    #Do the initial setup and get my object pool.  We begin with this pool of objects and keep adding
    #to it in groups of 10,000 or less until all objects are gone from the container.
    marker = ''
    objects = cont.get_objects(marker=marker)
    object_pool = [str(obj.name) for obj in objects]
    log.info("Container [%s] contains a total of [%d] objects." % (my_container,cont.object_count))
    log.info("Found [%d] object(s) to delete!" % len(object_pool))
    try:
        while objects:
            t1 = time.time()
            marker = str(objects[-1])
            try:
                objects = cont.get_objects(marker=marker)
                object_pool = [str(obj.name) for obj in objects]
                time.sleep(1)
                for obj in object_pool:
                    pool.spawn(delete_object, obj)
            except Exception as e:
                log.error("ERROR: %s" % e)
            marker = ''
            try:
                objects = cont.get_objects(marker=marker)
                object_pool = [str(obj.name) for obj in objects]
            except Exception as e:
                log.warning("No new objects to delete! Exiting!!")
                sys.exit()
            duration = time.time() - t1
            log.info("Time taken to delete [%d] objects: %0.2fs" % (len(object_pool), duration))
            if RUN_ONCE:
                sys.exit(0)
            log.info("Starting next batch of [%d] object(s)!" % len(object_pool))
            time.sleep(1)
    except Exception as e:
        log.error("There was an non-fatal error detected in event loop.  Moving on....")


if __name__ == "__main__":
    #Clear screen for initial output before launching delete
    os.system('clear')
    #Parse arguments
    parser = argparse.ArgumentParser(prog='Container Drainer', description='This script will DESTROY your container and is irreversible!!!',
        epilog="I hope you don't regret this!!!!!")
    parser.add_argument('--verbose','-v', action='store_false', help='Increase output to verbose', required=False )
    parser.add_argument('--username','-u', action='store', help='Rackspace Cloud username', required=True, type=str, dest='username')
    parser.add_argument('--api-key','-a', action='store', help='Rackspace Cloud API key', required=True, type=str, dest='key')
    parser.add_argument('--concurrency','-cc', action='store', default=100, help='This is the number of eventlets (think threads but NOT)\
        in the gevent pool.  Default concurrency [100]', required=False, type=int, dest='concurrency')
    parser.add_argument('--region', '-r', action='store', type=str, required=True, default='dfw', choices=['dfw','ord','iad','lon','hkg'],
        help='This is the region where your $CONTAINER is located', dest='region')
    parser.add_argument('--container', '-c', action='store', type=str,
        required=True, help='This is the container $NAME.  This container will be permanently deleted!!', dest='container')
    parser.add_argument('--snet', action='store_true', default=False, required=False, dest='snet',
        help='If your cloud server is in the same region as your container, this flag will use serviceNet')
    parser.add_argument('--run-once', action='store_true', required=False, default=False, dest='runonce',
        help='Delete a single batch of up to 10,000 objects and then exit')
    parser.add_argument('--log-level', action='store', type=str, required=False, default='INFO', dest='loglevel',
        choices=['info','warning','error','critical','debug'], help='Set the logging level.  Default value is INFO')
    args = parser.parse_args()
    username = args.username
    api_key = args.key
    my_region = args.region.upper()
    my_container = args.container
    sNet = args.snet
    if sNet:
        public = False
    else:
        public = True
    #for testing purposes, set 'RUN_ONCE' to True if you want to time a run deleting (a maximum of) 10,000 objects only
    RUN_ONCE = args.runonce
    #Set the logging level
    LOGLEVEL = args.loglevel.upper()
    #Set dDebug to 'Flase' so turn off Pyrax HTTP debug output.  This is the default but if logging is set to 'DEBUG' then
    #this value will be set to 'True' as well.
    hDebug = False
    #This will defind the number of threads/processes to spawn
    n_eventlets = args.concurrency
    #Get the number of CPU cores available.  Not being used currently but here for future release
    cpu_core_count = multiprocessing.cpu_count()
    #Define pool size.  This will be the number of co-routines/(not threads) to spawn
    pool = Pool(n_eventlets)
    confirm(username, api_key, my_region, my_container, public, RUN_ONCE, LOGLEVEL, n_eventlets)
    #-----------------------------------------------------------------
    #Set up logging here. To use "log.info("my message here").  Using function 'formatTime()' to make a
    #replace the './period' with a ',/comma' just before the milliseconds
    _formatTime = logging.Formatter.formatTime
    def formatTime(*args):
        return _formatTime(*args).replace(",", ".")

    logging.Formatter.formatTime = formatTime
    logging.basicConfig(
        format='[%(levelname)s] %(asctime)s %(message)s')
    log = logging.getLogger("Container_Drainer")
    levels=['INFO','WARNING','ERROR','CRITICAL','DEBUG']
    if args.loglevel:
        if LOGLEVEL == 'INFO':
            log.setLevel(logging.INFO)
        elif LOGLEVEL == 'WARNING':
            log.setLevel(logging.WARNING)
        elif LOGLEVEL == 'ERROR':
            log.setLevel(logging.ERROR)
        elif LOGLEVEL == 'CRITICAL':
            log.setLevel(logging.CRITICAL)
        elif LOGLEVEL == 'DEBUG':
            log.setLevel(logging.DEBUG)
            #set Pyrax HTTP debug to True
            hDebug = True
    else:
        log.setLevel(logging.INFO)
    #-----------------------------------------------------------------
    log.info("Preparing pyrax environment...")
    #Set identity authenticate to Rackspace
    pyrax.set_setting('identity_type', 'rackspace')
    pyrax.set_credentials(username, api_key, authenticate=True)
    pyrax.set_http_debug(hDebug)
    pyrax.set_setting('verify_ssl', False)

    #Create connection object for cloud files and 'get' our container targeted for draining
    try:
        #Create connection to specified region
        cfiles = pyrax.connect_to_cloudfiles(region=my_region, public=public)
        log.info("Successfully connected to region [%s]" % my_region)
    except Exception as e:
        log.error("Unable to connect to [%s] region!" % my_region)
        sys.exit(1)
    try:
        #Get the container we want to delete/drain
        cont = cfiles.get_container(my_container)
        log.info("Successfully performed GET on container [%s]" % my_container)
    except Exception as e:
        log.error("Unable to GET container object.  Container [%s] may not exist, or it is in another region!" % my_container)
        sys.exit(1)

    #Set 'drain' variable to True to begin our execution process
    log.info("Setting 'drain' to True")
    drain = True
    time.sleep(3)
    while drain:
        try:
            log.info("Begin run() execution")
            run()
            log.info("Deleting container [%s]" % my_container)
            cont.delete()
            log.info("Container deleted!! Exiting!!!")
            drain = False
            sys.exit()
        except Exception as e:
            log.error(e)
            print "Error deleting container.  Checking for scraps in container..."
            run()
        finally:
            drain = False
            sys.exit()
