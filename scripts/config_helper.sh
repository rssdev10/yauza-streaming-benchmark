ROOT_DIR=`readlink -f $(dirname "$0")/..`
CONF_DIR=$ROOT_DIR/conf
SCRIPTS_DIR=$ROOT_DIR/scripts

source $CONF_DIR/cmd_path.sh

get_slaves() {
  echo `cat $CONF_DIR/slaves | grep -v "^\s*[#;]"`
}

get_slaves_and_masters() {
  local slaves=$(get_slaves)
  slaves=`echo -e "$slaves\n\`hostname\`" | grep -v localhost | sort -u`
  echo $slaves
}
