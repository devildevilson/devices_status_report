require("dotenv").config({ path: `${__dirname}/.env` });
const egsv_api = require("./apis/egsv");
const zabbix_api = require("./apis/zabbix");
const schedule = require('node-schedule');
const axios = require("axios");
const fs = require("fs");

const egsv_sko = new egsv_api({
  host: process.env.EGSV_HOST3,
  port: process.env.EGSV_PORT3,
  user: process.env.EGSV_USER3,
  pass: process.env.EGSV_PASS3
});

const zabbix_sko = new zabbix_api({ host: "172.20.21.200", token: process.env.ZABBIX_SKO_API_TOKEN });

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
const cur_dir = __dirname;
const telegram_bot_id = load_file_content(`${cur_dir}/telegram_bot_id`);
const telegram_chat_id = load_file_content(`${cur_dir}/telegram_chat_id`);

async function broadcast_message() {
  const current_date = new Date();
  const last_5h = (new Date()).setTime(current_date.getTime() - 5*60*60*1000);

  const ret = await egsv_sko.method("rtms.report.list", {
    filter: {
      datetime: {
        $gte: make_sane_time_string(last_5h),
        $lte: make_sane_time_string(current_date)
      }
    },
    group: { hour: true },
    include: [ 'cameras', 'last_datetimes' ]
  });

  const obj = {};
  ret.cameras.forEach((elem) => { obj[elem.id] = elem; });

  let zabbix_egsv_cam_id = {};
  let zabbix_problem_arr = [];

  {
    const problems = await zabbix_sko.method("problem.get", { 
      groupids: [ 28, 31, 42 ],
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
      hostids: host_ids,
    });

    zabbix_problem_arr = macros.filter(el => el.macro === "{$EGSVCAMERAID}").map(
      el => { 
        return { 
          cam_id: el.value, 
          host_id: el.hostid, 
          host_name: el.hosts[0].name, 
          host_short: el.hosts[0].host,
          egsv_name: obj[el.value].name,
          problem_since: host_problem_time[el.hostid]
        } 
      }
    );
    zabbix_problem_arr.forEach(el => zabbix_egsv_cam_id[el.cam_id] = true);
    zabbix_problem_arr.sort((a, b) => strcmp(a.host_short, b.host_short));
  }

  let arr = [];
  for (const [ key, stats ] of Object.entries(ret.stats)) {
  	if (zabbix_egsv_cam_id[key]) continue;
  	
    let problem_start = undefined;

    if (stats.length > 0) {
      const data = stats[stats.length-1];
      const date = new Date(data.datetime);
      const cur_str = make_sane_date_string(current_date);
      const date_str = make_sane_date_string(date);
      if (cur_str !== date_str || current_date.getHours() !== date.getHours()) {
        problem_start = new Date(data.datetime);
      }
    } else {
      problem_start = last_5h;
    }
//    if (stats.length < 4) {
//      problem_start = stats.length !== 0 ? stats[stats.length-1].datetime : last_5h;
//    }

    if (problem_start) {
      arr.push({ problem_start, camera: obj[key] });
    }
  }

  arr.sort((a,b) => strcmp(a.camera.name, b.camera.name));

  let zabbix_str = "";
  let counter = 1;
  for (const elem of zabbix_problem_arr) {
    const date = make_sane_time_string(parse_unix_date(elem.problem_since));
  	const local_str = `${counter}) ${elem.host_name} не работает с ${date}\n`;
  	counter += 1;
  	zabbix_str += local_str;
  }

  let egsv_str = "";
  for (const elem of arr) {
    const problem_date = new Date(elem.problem_start);
    const start_hours = problem_date.getHours();
    const local_str = `${counter}) ${elem.camera.name} не работает с ${start_hours} часов\n`;
    counter += 1;
    egsv_str += local_str;
  }

  let final_str = `\nZabbix:\n${zabbix_str}\nEGSV:\n${egsv_str}`;
  if (zabbix_problem_arr.length === 0 && arr.length === 0) final_str = "\nПроблем нет";

  const msg = `chat_id=${telegram_chat_id}&text=\nСКО Отчет ${make_sane_date_string(current_date)}\n${final_str.trim()}`;
  const t_ret = await axios.post(`https://api.telegram.org/bot${telegram_bot_id}/sendMessage`, msg);
}

//const job1 = schedule.scheduleJob('30 9 * * *', async function(){
//  await broadcast_message();
//  const time = make_sane_time_string(new Date());
//  console.log(`[${time}] send report`);
//});

//const job2 = schedule.scheduleJob('30 15 * * *', async function(){
//  await broadcast_message();
//  const time = make_sane_time_string(new Date());
//  console.log(`[${time}] send report`);
//});

const time = make_sane_time_string(new Date());
console.log(`[${time}] send report`);
broadcast_message();
