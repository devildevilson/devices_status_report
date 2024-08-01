require("dotenv").config({ path: `${__dirname}/.env` });
const egsv_api = require("./apis/egsv");
const zabbix_api = require("./apis/zabbix");
const axios = require("axios");
const fs = require("fs");

const egsv_sko = new egsv_api({
  host: process.env.EGSV_HOST,
  port: process.env.EGSV_PORT,
  user: process.env.EGSV_USER,
  pass: process.env.EGSV_PASS
});

const zabbix_sko = new zabbix_api({ host: process.env.ZABBIX_HOST, token: process.env.ZABBIX_API_TOKEN });

const make_good_num = num => num < 10 ? "0"+num : ""+num;

function make_sane_time_string(date) {
  const final_date = new Date(date);
  const y = final_date.getFullYear();
  const m = make_good_num(final_date.getMonth()+1);
  const d = make_good_num(final_date.getDate());
  const H = make_good_num(final_date.getHours());
  const M = make_good_num(final_date.getMinutes());
  const S = make_good_num(final_date.getSeconds());
  return `${y}-${m}-${d} ${H}:${M}:${S}`;
}

function make_sane_date_string(date) {
  const final_date = new Date(date);
  const y = final_date.getFullYear();
  const m = make_good_num(final_date.getMonth()+1);
  const d = make_good_num(final_date.getDate());
  return `${y}.${m}.${d}`;
}

function load_file_content(path) {
  return fs.readFileSync(path, { encoding: 'utf8', flag: 'r' });
}

const strcmp = (a,b) => (a < b ? -1 : +(a > b));
const parse_unix_date = (timestamp) => new Date(timestamp * 1000);

const minimum_events_within_hour = 1;
const telegram_bot_id = process.env.TELEGRAM_BOT_API_TOKEN;
const telegram_chat_id = process.env.TELEGRAM_BOT_CHAT;
const hour = 60 * 60 * 1000;
const day = 24 * hour;

async function broadcast_message() {
  const current_date = new Date();
  const last_5h  = (new Date()).setTime(current_date.getTime() - hour * 5);
  const last_30d = (new Date()).setTime(current_date.getTime() - day * 30);

  const ret = await egsv_sko.method("rtms.report.list", {
    filter: {
      datetime: {
        $gte: make_sane_time_string(last_5h),
        $lte: make_sane_time_string(current_date)
      }
    },
    //group: { hour: true },
    include: [ 'cameras', 'last_datetimes' ]
  });

  const obj = {};
  ret.cameras.forEach((elem) => { obj[elem.id] = elem; });

  const zabbix_groupids = (process.env.ZABBIX_GROUPIDS).split(",").map(el => Number(el.trim()));
  let zabbix_egsv_cam_id = {};
  let zabbix_problem_arr = [];

  {
    const problems = await zabbix_sko.method("problem.get", { 
      groupids: zabbix_groupids,
      severities: [ 4 ]
    });

    const event_ids = problems.map(el => el.eventid);
    const events = await zabbix_sko.method("event.get", { 
      eventids: event_ids,
      severities: [ 4 ],
      selectHosts: "extend",
    });

    let host_problem_time = {};
    events.forEach(el => el.hosts.forEach(h => host_problem_time[h.hostid] = el.clock));
    const host_ids_arr = events.map(el => el.hosts.map(el1 => el1.hostid));
    const host_ids = [].concat.apply([], host_ids_arr);
    const macros = await zabbix_sko.method("usermacro.get", {
      selectHosts: "extend",
      selectHostGroups: "extend",
      hostids: host_ids,
    });

    zabbix_problem_arr = macros.filter(el => el.macro === "{$EGSVCAMERAID}").map(
      el => { 
        return { 
          cam_id: el.value, 
          host_id: el.hostid, 
          host_name: el.hosts[0].name, 
          host_short: el.hosts[0].host,
          egsv_name: obj[el.value] ? obj[el.value].name : undefined,
          problem_since: host_problem_time[el.hostid],
        } 
      }
    );
    zabbix_problem_arr.forEach(el => zabbix_egsv_cam_id[el.cam_id] = true);
    zabbix_problem_arr.sort((a, b) => strcmp(a.host_short, b.host_short));
  }

  // тут теперь имеет смысл пройтись по каждой камере и вернуть последнее событие для камеры
  let promises_arr = [];
  for (const [ key, stats ] of Object.entries(ret.stats)) {
    if (zabbix_egsv_cam_id[key]) continue;
    const camera = obj[key];
    const p = egsv_sko.method("rtms.number.list", {
      filter: {
        datetime: {
          $gte: make_sane_time_string(last_30d),
          $lte: make_sane_time_string(current_date)
        },
        camera: { $in: [ camera.id ] }
      },
      limit: 1,
      sort: { datetime: 'desc' }
      //include: [ 'cameras', 'last_datetimes' ]
    });

    promises_arr.push([ camera.id, p ]);
  }

  const events_arr = (await Promise.all(promises_arr.map(el => el[1]))).map((el, index) => [ promises_arr[index][0], el.numbers ? el.numbers[0] : undefined ]);

  let arr = [];
  for (const [ camera_id, event ] of events_arr) {
    const camera = obj[camera_id];
    if (!event) {
      arr.push({ problem_start: last_30d, camera });
      continue;
    }

    const event_date = new Date(event.datetime);
    const diff = Math.abs(current_date - event_date);
    // если больше чем 4 часа
    if (diff > 4 * hour) { 
      arr.push({ problem_start: event_date, camera }); 
    }
  }

  arr.sort((a,b) => strcmp(a.camera.name, b.camera.name));

  let zabbix_str = "";
  for (const elem of zabbix_problem_arr) {
    const date = make_sane_time_string(parse_unix_date(elem.problem_since));
  	const local_str = `${counter}) ${elem.host_name} не работает с ${date}\n`;
  	counter += 1;
  	zabbix_str += local_str;
  }
  if (zabbix_problem_arr.length > 0) zabbix_str = "\nZabbix:\n" + zabbix_str;

  let egsv_str = "";
  for (const elem of arr) {
    const date = make_sane_time_string(elem.problem_start);
    const local_str = `${counter}) ${elem.camera.name} последнее событие ${date}\n`;
    counter += 1;
    egsv_str += local_str;
  }
  if (arr.length > 0) egsv_str = "\nEGSV:\n" + egsv_str;

  let final_str = `${zabbix_str}${egsv_str}`;
  if (zabbix_problem_arr.length === 0 && arr.length === 0) final_str = "\nПроблем нет";

  const msg = `chat_id=${telegram_chat_id}&text=\n${process.env.REPORT_OPENING} ${make_sane_date_string(current_date)}\n${final_str.trim()}`;
  const t_ret = await axios.post(`https://api.telegram.org/bot${telegram_bot_id}/sendMessage`, msg);
}

// теперь используем cron для запуска скрипта

const time = make_sane_time_string(new Date());
console.log(`[${time}] send report`);
broadcast_message();
