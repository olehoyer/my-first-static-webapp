importScripts("https://cdn.jsdelivr.net/pyodide/v0.21.3/full/pyodide.js");

function sendPatch(patch, buffers, msg_id) {
  self.postMessage({
    type: 'patch',
    patch: patch,
    buffers: buffers
  })
}

async function startApplication() {
  console.log("Loading pyodide!");
  self.postMessage({type: 'status', msg: 'Loading pyodide'})
  self.pyodide = await loadPyodide();
  self.pyodide.globals.set("sendPatch", sendPatch);
  console.log("Loaded!");
  await self.pyodide.loadPackage("micropip");
  const env_spec = ['https://cdn.holoviz.org/panel/0.14.0/dist/wheels/bokeh-2.4.3-py3-none-any.whl', 'https://cdn.holoviz.org/panel/0.14.0/dist/wheels/panel-0.14.0-py3-none-any.whl', 'holoviews>=1.15.1', 'holoviews>=1.15.1', 'hvplot', 'pandas', 'param']
  for (const pkg of env_spec) {
    const pkg_name = pkg.split('/').slice(-1)[0].split('-')[0]
    self.postMessage({type: 'status', msg: `Installing ${pkg_name}`})
    await self.pyodide.runPythonAsync(`
      import micropip
      await micropip.install('${pkg}');
    `);
  }
  console.log("Packages loaded!");
  self.postMessage({type: 'status', msg: 'Executing code'})
  const code = `
  
import asyncio

from panel.io.pyodide import init_doc, write_doc

init_doc()

import xml.etree.ElementTree as ET
import pandas as pd
import datetime as dt
#from dateutil.relativedelta import *
#import dateutil
from datetime import timedelta


import re
from io import StringIO
import param

import hvplot.pandas
import holoviews as hv


import io
import panel as pn

pn.extension('tabulator')


speed_limit = 175   # Speed limit for steps/minute

# Calcuate speed based on duration and count
def speed_sec(steps, duration_min):
    return round((steps / duration_min), 2)

# Calculate speed and clean up
def calc_duration_speed(upd_filt):
    # Calculate duration from start to end
    upd_filt.loc[:, ['duration_sec']] = upd_filt.apply(lambda row: 
                                        (pd.Timedelta(row.endDate - row.startDate).seconds), axis=1)

    # Remove any durations that are zero
    upd_filt = upd_filt.loc[upd_filt['duration_sec'] != 0]

    # Calcuate speed based on steps and duration
    upd_filt.loc[:, ['speed']] = upd_filt.apply(lambda row: 
                                            round(speed_sec(row.value, row.duration_sec/60)), axis=1)

    return upd_filt

class ActivityApp(param.Parameterized):
    data = param.DataFrame()

    file_input = param.Parameter()

    date_select = param.Parameter()

    source_select = param.Parameter()

    def __init__(self, **params):
        super().__init__(file_input=pn.widgets.FileInput(accept='.csv,.xml'), **params)
        super().__init__(source_select=pn.widgets.CheckButtonGroup(name='Select source',value=[],options=[]), **params)
        super().__init__(date_select=pn.widgets.DatePicker(name='Pick date'), **params)
        self.dis_plot = pn.Column("empty")
        self.dis_plot_day = pn.Column("empty")

    def find_dev(self, device_str, source_name):
        if source_name == "Connect":
            return 'GarminWatch'
        try:
            if "name:Apple Watch" in device_str:
                return 'AppleWatch'
            elif "name:iPhone" in device_str:
                return 'iPhone'
            elif "samsung" in device_str:
                return 'Samsung'
            elif "uawei" in device_str:
                return 'Huawei'
        except: ""
        return 'Unknown'

    @pn.depends("file_input.value", watch=True)
    def _parse_apple(self):
        if re.search(r'csv$',self.file_input.filename):

            s=str(self.file_input.value,'utf-8')

            string_io = StringIO(s) 

            df_steps=pd.read_csv(string_io)
            df_steps['startDate'] = pd.to_datetime(df_steps['startDate'])
            df_steps['startDate'] = df_steps['startDate'].apply(lambda x: x.replace(tzinfo=None))
            df_steps['endDate'] = pd.to_datetime(df_steps['endDate'])
            df_steps['endDate'] = df_steps['endDate'].apply(lambda x: x.replace(tzinfo=None))
            df_steps.rename(columns={'DataStreamId (device id)':'device'}, inplace=True)
            df_steps['deviceType'] = df_steps.apply(lambda x: self.find_dev(x.device, x.sourceName), 
                                                    axis=1)

#            print(df_steps.head(1))
#            print(df_steps.dtypes)
            list_source = df_steps['deviceType'].unique()
            self.source_select.options = list_source.tolist()
            self.source_select.value = list_source.tolist()

            self.data = df_steps

        elif re.search(r'xml$',self.file_input.filename):
            print('xml file')

            s=str(self.file_input.value,'utf-8')

            string_io = StringIO(s)

            tree = ET.parse(string_io)
            root = tree.getroot()
            record_list = [x.attrib for x in root.iter('Record')]

            record_data = pd.DataFrame(record_list)
            print(record_data.head(1))

            # Set date types
            for col in ['creationDate', 'startDate', 'endDate']:
                record_data[col] = pd.to_datetime(record_data[col])

            # value is numeric, NaN if fails
            record_data['value'] = pd.to_numeric(record_data['value'], errors='coerce')

            # some records do not measure anything, just count occurences
            # filling with 1.0 (= one time) makes it easier to aggregate
            record_data['value'] = record_data['value'].fillna(1.0)

            # shorter observation names
            record_data['type'] = record_data['type'].str.replace('HKQuantityTypeIdentifier', '')
            record_data['type'] = record_data['type'].str.replace('HKCategoryTypeIdentifier', '')
            self.data = record_data
        
        else:
            print("Type not recognized")

    @pn.depends('data', 'source_select', watch=True)
    def get_plot(self):
        self.dis_plot.pop(0)
        list_source = self.source_select.value
        source_info = ""
        df_plot = ""

        for i in range(len(list_source)):
            source_min = self.data[(self.data['deviceType'] == list_source[i])]['startDate'].min()
            source_max = self.data[(self.data['deviceType'] == list_source[i])]['startDate'].max()
            source_info = f"Source: {list_source[i]} \\n\\n   start {source_min} end {source_max}\\n" + "\\n" + source_info
            df_source = self.data[(self.data['deviceType'] == list_source[i])]
            df_source.rename(columns= {'value':list_source[i]},inplace=True)
            if i == 0:
                df_plot = df_source.resample('D', on='startDate').agg({list_source[i]:'sum'}
                            ).hvplot.bar(x='startDate', y=list_source[i]).opts(shared_axes=False)
            else:
                df_plot = df_source.resample('D', on='startDate').agg({list_source[i]:'sum'}
                            ).hvplot.bar(x='startDate', y=list_source[i]).opts(shared_axes=False) * df_plot

        self.dis_plot.insert(0, pn.Column(
                                    df_plot,
                                    source_info)
                                    )

    @pn.depends('date_select.value', 'source_select.value', watch=True)
    def get_day_plot(self):
        if self.date_select.value == None:
            return

        start = pd.to_datetime(self.date_select.value)
#        start = pd.to_datetime(self.date_select.value, utc=True)
        end = start + timedelta(days=1)
        workouts = self.data[(self.data["startDate"] >= start) & (self.data["startDate"] < end)]
#        print(workouts.head(2))
        list_source = self.source_select.value
        source_cnt_info = pn.pane.Markdown("")
        df_plot = ""

        for i in range(len(list_source)):
            df_source = workouts[(workouts['deviceType'] == list_source[i])]
            df_source = calc_duration_speed(df_source)
            df_source = df_source[(df_source['speed'] <= speed_limit)]
            df_spd_exp = pd.DataFrame(columns={'startDate','speed','duration_sec'})
            for index, row in df_source.iterrows():
                df_spd_exp = df_spd_exp.append({'startDate': row.startDate,'speed': row.speed,
                                                'duration_sec':row.duration_sec}, ignore_index=True)
                df_spd_exp = df_spd_exp.append({'startDate': row.endDate,'speed': 0}, ignore_index=True)
            df_spd_exp.rename(columns= {'speed':list_source[i]},inplace=True)

            source_cnt_info = pn.Column(source_cnt_info, 
                                    pn.pane.Markdown(f"{list_source[i]} entries {len(df_spd_exp)}\\n" + "\\n"),
                                    pn.widgets.Tabulator(df_source.sort_values('startDate'), pagination='local', page_size=5, show_index=False)
                                    )

            if i == 0:
                if len(df_spd_exp) > 0:
                    df_plot = df_spd_exp.hvplot.step(x='startDate', y=list_source[i],
                                            value_label='speed', legend='top').opts(shared_axes=False)

            else:
                if len(df_spd_exp) > 0:
                    df_plot_next = df_spd_exp.hvplot.step(x='startDate', y=list_source[i],
                                            value_label='speed', legend='top').opts(shared_axes=False)
                    df_plot = df_plot * df_plot_next
        try:
            self.dis_plot_day.pop(0)
            self.dis_plot_day.insert(0, pn.Column(df_plot, source_cnt_info))
        except: ""


    def view(self):
        return pn.Row(pn.Column(self.file_input, self.source_select, self.date_select),
                      pn.Column(self.dis_plot, self.dis_plot_day, sizing_mode="stretch_width"))
        



activity_app = ActivityApp()
activity_app_view = activity_app.view()


pn.template.FastListTemplate(site="Panel", title="Activity Source Analysis", 
#                             main=[file_input]).servable();
                             main=[activity_app_view]).servable();

await write_doc()
  `
  const [docs_json, render_items, root_ids] = await self.pyodide.runPythonAsync(code)
  self.postMessage({
    type: 'render',
    docs_json: docs_json,
    render_items: render_items,
    root_ids: root_ids
  });
}

self.onmessage = async (event) => {
  const msg = event.data
  if (msg.type === 'rendered') {
    self.pyodide.runPythonAsync(`
    from panel.io.state import state
    from panel.io.pyodide import _link_docs_worker

    _link_docs_worker(state.curdoc, sendPatch, setter='js')
    `)
  } else if (msg.type === 'patch') {
    self.pyodide.runPythonAsync(`
    import json

    state.curdoc.apply_json_patch(json.loads('${msg.patch}'), setter='js')
    `)
    self.postMessage({type: 'idle'})
  } else if (msg.type === 'location') {
    self.pyodide.runPythonAsync(`
    import json
    from panel.io.state import state
    from panel.util import edit_readonly
    if state.location:
        loc_data = json.loads("""${msg.location}""")
        with edit_readonly(state.location):
            state.location.param.update({
                k: v for k, v in loc_data.items() if k in state.location.param
            })
    `)
  }
}

startApplication()