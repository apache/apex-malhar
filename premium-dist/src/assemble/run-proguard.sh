#!/bin/bash

# this script reads the maven generated classpaths and run proguard appropriately
# we are not using the maven proguard plugin because it's terribly buggy. (injars being treated as outjars, outjars not working with filters for example)

RTJAR=$JAVA_HOME/jre/lib/rt.jar
PWD=`pwd -P`
VERSION=$MALHAR_BUILD_VERSION

if [ "$VERSION" = "" ]
then
    echo "ERROR: Environment variable MALHAR_BUILD_VERSION needs to be set!"
    exit 1;
fi

cat > proguard.conf <<EOF
-dontshrink
-dontoptimize
-dontwarn com.malhartech.contrib.**
-dontwarn org.apache.hadoop.**
-keeppackagenames com.malhartech.**
-keep public class com.malhartech.contrib.** { *; }
-keep public class com.malhartech.demos.** { *; }
-keep public class com.malhartech.lib.** { *; }
-keep public class com.malhartech.daemon.Daemon { public *; }
-keep public class com.malhartech.api.** { *; }
-keep public class com.malhartech.bufferserver.Buffer { *; }
-keep public class com.malhartech.bufferserver.Buffer$** { *; }
-keep public class com.malhartech.util.VersionInfo { *; }
-keep public interface com.malhartech.api.** { *; }
-keep public interface com.malhartech.annotation.** { *; }
-keep public class com.malhartech.stram.cli.StramAppLauncher { 
    public static java.lang.String runApp(**); 
}
-keep public class com.malhartech.stram.webapp.*Info { public protected *; }
-keep public class com.malhartech.stram.webapp.*Info$** { public protected *; }
-keep public interface com.malhartech.stram.cli.StramAppLauncher$AppConfig
-keepclasseswithmembers class ** { 
    public static void main(java.lang.String[]); 
}
-keepclassmembers class * implements java.io.Serializable {
    static final long serialVersionUID;
    private static final java.io.ObjectStreamField[] serialPersistentFields;
    !static !transient <fields>;
    !private <fields>;
    !private <methods>;
    private void writeObject(java.io.ObjectOutputStream);
    private void readObject(java.io.ObjectInputStream);
    java.lang.Object writeReplace();
    java.lang.Object readResolve();
}
-keepclassmembernames class ** { 
    public static final long versionID; 
}
-keepclassmembers enum * { *; }
-keepattributes *Annotation*,EnclosingMethod,Exceptions
-keepdirectories **
-adaptresourcefilenames **.properties,**.gif,**.jpg
-adaptresourcefilecontents **.properties,META-INF/MANIFEST.MF
-printmapping proguard_map.txt
-printseeds proguard_seeds.txt
-libraryjars $RTJAR
EOF

cat mvn-generated-classpath | tr : '\n' > mvn-generated-classpath.tmp

for path in `cat mvn-generated-classpath.tmp`
do
   if [[ "$path" == *malhar-stram* ]]
   then
       echo "-injars $path"  >> proguard.conf
   elif [[ "$path" == *malhar* ]]
   then
       echo "-injars $path(!META-INF/MANIFEST.MF)" >> proguard.conf
   else
       echo "-libraryjars $path"  >> proguard.conf
   fi
done

cat >> proguard.conf <<EOF
-outjars malhar-contrib-$VERSION.jar(**/malhar-contrib/**,com/malhartech/contrib/**)
-outjars malhar-bufferserver-$VERSION.jar(**/malhar-bufferserver/**,com/malhartech/bufferserver/**)
-outjars malhar-daemon-$VERSION.jar(**/malhar-daemon/**,com/malhartech/daemon/**)
-outjars malhar-demos-$VERSION.jar(**/malhar-demos/**,com/malhartech/demos/**)
-outjars malhar-library-$VERSION.jar(**/malhar-library/**,com/malhartech/lib/**)
-outjars malhar-stram-$VERSION.jar
EOF

rm mvn-generated-classpath.tmp
proguard @proguard.conf -verbose

for module in contrib bufferserver daemon demos library stram
do
    cp -p malhar-$module-$VERSION.jar $HOME/.m2/repository/com/malhartech/malhar-$module/$VERSION/
    mkdir -p ../../$module/target
    cp -p malhar-$module-$VERSION.jar ../../$module/target
done
