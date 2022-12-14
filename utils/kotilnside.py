"""Kotlinside related function"""
import os

import jpype

from . import paths, strings

os.environ["JAVA_HOME"] = paths.JAVA_HOME
CLASS_PATH = f"{paths.KOTLIN_HOME}/{paths.KOTLIN_FILENAME}"

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
                   strings.JAVA_PATH_ARGS,
                   convertStrings=True)


def get_auth():
    jvm_init()
    kotlininside = jpype.JPackage(strings.KOTLININSIDE_PACKAGE_NAME)
    user = kotlininside.session.user.Anonymous
    inside = kotlininside.KotlinInside
    http = kotlininside.http.DefaultHttpClient

    _ = inside.createInstance(
        user(strings.KOTLININSIDE_USERNAME, strings.KOTLININSIDE_PASSWORD),
        http(True, True))
    auth = kotlininside.security.Auth()
    return auth


def generate_app_id(auth):
    hased_app_key = auth.generateHashedAppKey()
    app_id = auth.fetchAppId(hased_app_key)
    return app_id


def jvm_shutdown():
    jpype.shutdownJVM(jpype.getDefaultJVMPath(), strings.JAVA_PATH_ARGS)
