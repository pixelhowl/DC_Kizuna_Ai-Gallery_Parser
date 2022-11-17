import os

import jpype  # pip3 install JPype1

from . import *

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
CLASS_PATH = KOTLIN_HOME + "/KotlinInside-1.14.6-fat.jar"


def run_once(f):

    def wrapper(*args, **kwargs):
        if not wrapper.has_run:
            wrapper.has_run = True
            return f(*args, **kwargs)

    wrapper.has_run = False
    return wrapper


@run_once
def jvm_init():
    jpype.startJVM(
        jpype.getDefaultJVMPath(),
        "-Djava.class.path={classpath}".format(classpath=CLASS_PATH),
        convertStrings=True,
    )


def get_auth():
    jvm_init()
    kotlininside = jpype.JPackage("be.zvz.kotlininside")
    user = kotlininside.session.user.Anonymous
    inside = kotlininside.KotlinInside
    http = kotlininside.http.DefaultHttpClient

    _ = inside.createInstance(user("ㅇㅇ", "zhxmfflsakstp"), http(True, True))
    auth = kotlininside.security.Auth()
    return auth


def generate_app_id(auth):
    hashedAppKey = auth.generateHashedAppKey()
    app_id = auth.fetchAppId(hashedAppKey)
    return app_id


def jvm_shutdown():
    jpype.shutdownJVM(
        jpype.getDefaultJVMPath(),
        "-Djava.class.path={classpath}".format(classpath=CLASS_PATH),
        convertStrings=True,
    )
