"""Kotlinside related function"""
import os

import jpype

from . import KOTLIN_HOME

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
CLASS_PATH = KOTLIN_HOME + "/KotlinInside-1.14.6-fat.jar"

# pylint: disable=too-many-function-args


def run_once(f):

    def wrapper(*args, **kwargs):
        if not wrapper.has_run:
            wrapper.has_run = True
            return f(*args, **kwargs)

    wrapper.has_run = False
    return wrapper


@run_once
def jvm_init():
    jpype.startJVM(jpype.getDefaultJVMPath(),
                   f"-Djava.class.path={CLASS_PATH}",
                   convertStrings=True)


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
    hased_app_key = auth.generateHashedAppKey()
    app_id = auth.fetchAppId(hased_app_key)
    return app_id


def jvm_shutdown():
    jpype.shutdownJVM(jpype.getDefaultJVMPath(),
                      f"-Djava.class.path={CLASS_PATH}")
