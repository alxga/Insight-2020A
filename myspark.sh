if [ "$#" -lt 1 ]; then
  echo "Usage myspark.sh <PYTHON SCRIPT> [--on-master]"
  exit 1
fi

LAUNCH_PYSCRIPT="$1"
shift

onMaster=false
extraArgs=""
while [ $# -ne 0 ]
do
    arg="$1"
    case "$arg" in
        --on-master)
            onMaster=true
            ;;
        *)
            extraArgs="${extraArgs} $arg"
            ;;
    esac
    shift
done

ZIP_NAME=".reqs4spark.zip"
PY_PACKAGES="common third_party spk_updatevehpospq.py"

zip_my_requirements()
{
  zip -r - $PY_PACKAGES > "$ZIP_NAME";
}

# protoc gtfs-realtime.proto --python_out=.
zip_my_requirements

args="--py-files=${ZIP_NAME}"
if [ "$onMaster" = "true" ] ; then
  args="${args} --master spark://${SPARKM}:7077"
fi
echo $args
PYSPARK_DRIVER_PYTHON="$HOME/venv/bin/python"
spark-submit $args $extraArgs "$LAUNCH_PYSCRIPT"

