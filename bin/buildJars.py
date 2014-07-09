#! /usr/bin/env python
# encoding: utf-8

import os
import sys
import subprocess
import re
import shutil

if len(sys.argv) <= 1:
    print("Usage: {scriptName} <pathToGiraphDir>".format(
        scriptName=os.path.basename(__file__)))
    sys.exit(1)

giraphDir = os.path.abspath(sys.argv[1])
okapiDir = os.path.join(os.getcwd(), "..")
okapiTargetDir = os.path.join(okapiDir, "target")
okapiJarDir = os.path.join(okapiDir, "jars")

if not os.path.isdir(okapiJarDir):
    os.mkdir(okapiJarDir)

okapiJarRegex = re.compile(
    "^okapi-([\d.]+)-SNAPSHOT-jar-with-dependencies.jar"
)

builds = [
    {
        "computation": "mr1",
        "withGiraph": True,
        "destOkapiJar": "okapi-giraph-mr1-{version}.jar",
        "giraphBuildCustomArguments": "",
        "okapiBuildCustomArguments": ""
    },
    {
        "computation": "mr2",
        "withGiraph": True,
        "destOkapiJar": "okapi-giraph-mr2-{version}.jar",
        "giraphBuildCustomArguments": "-Phadoop_2",
        "okapiBuildCustomArguments": "-Phadoop_yarn"
    },
    #{
        #"computation": "yarn",
        #"withGiraph": True,
        #"destOkapiJar": "okapi-giraph-yarn-{version}.jar",
        #"giraphBuildCustomArguments": "-Phadoop_yarn -Dhadoop.version=2.2.0",
        #"okapiBuildCustomArguments": "-Phadoop_yarn"
    #},
    {
        "computation": "all",
        "withGiraph": False,
        "destOkapiJar": "okapi-{version}.jar",
        "giraphBuildCustomArguments": "",
        "okapiBuildCustomArguments": "-Phadoop_yarn -Dgiraph.scope=provided"
    },
]


def compileGiraph(build):
    os.chdir(giraphDir)
    print("Compiling Giraph for {0}".format(build['computation']))
    subprocess.call(
        "mvn {0} -DskipTests clean install"
        .format(build['giraphBuildCustomArguments']), shell=True)


def compileOkapi(build):
    os.chdir(okapiDir)
    print("Compiling Okapi for {0}".format(build['computation']))
    subprocess.call(
        "mvn {0} -DskipTests clean package"
        .format(build['okapiBuildCustomArguments']), shell=True)


def moveJar(build):
    os.chdir(okapiTargetDir)

    sourceJar = None
    version = None

    for f in os.listdir("."):
        match = okapiJarRegex.match(f)

        if match:
            sourceJar = f
            version = match.group(1)
            break

    if sourceJar is None:
        print("Unable to find original Okapi jar matching {0}".format(
            repr(okapiJarRegex)))
        return

    destinationJar = build["destOkapiJar"].format(version=version)

    print("Copying {0} to {1}".format(sourceJar, destinationJar))

    destinationJarFullPath = os.path.join(okapiJarDir, destinationJar)

    shutil.copy(sourceJar, destinationJarFullPath)


for build in builds:
    if build['withGiraph']:
        compileGiraph(build)
    compileOkapi(build)
    moveJar(build)

os.chdir(okapiDir)
print("Finished!")
