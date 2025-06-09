# ODB Open API to query GLODAP v2.2023 data

[![DOI](https://zenodo.org/badge/doi/10.5281/zenodo.15606826.svg)](https://doi.org/10.5281/zenodo.15606826)

### Swagger API Doc

[ODB GLODAP API manual and live try-out](https://api.odb.ntu.edu.tw/hub/swagger?node=odb_glodap_v1)

### Usage

This API provides structured access to GLODAPv2.2023 data, including both [cruise metadata](https://www.ncei.noaa.gov/access/ocean-carbon-acidification-data-system/oceans/GLODAPv2_2023/cruise_table_v2023.html) and discrete bottle measurements. Two main endpoints are available:

#### `/glodap/v2/2023/cruise`

Query cruise metadata with flexible filters:

* `start`, `end`: filter cruises by start and end date (e.g. `start=1990-01-01`).
* `pi`: fuzzy search for PI names using wildcards (e.g. `pi=*Liu*,*Gong*`).
* `region`, `ship`: filter by region or ship name (case-insensitive, supports wildcards).
* `field`: specify which PI fields to include or filter (e.g. `field=chief,carbon,hydrography`; see available fields in [cruise metadata](https://www.ncei.noaa.gov/access/ocean-carbon-acidification-data-system/oceans/GLODAPv2_2023/cruise_table_v2023.html)). Use `false` to disable extra PI columns.
* `append`: add extra metadata columns such as `qc_details`, `map`, etc. Abbreviations are supported:
  `file`, `qc`, `map`, `metadata`, `ref` → corresponding to `data_files`, `qc_details`, `map`, `metadata_report`, `cruise_references`.
* `format`: output format (`json` or `csv`; default is `json`).

📌 **Example:**
Query Arctic cruises with carbon or chief scientists matching “Kelly” or “Schlosser”, and include QC and map metadata:

```
https://ecodata.odb.ntu.edu.tw/glodap/v2/2023/cruise?pi=Kelly*,Schlosser&field=chief,carbon&region=Arctic&append=qc,map&format=json
```

#### `/glodap/v2/2023`

Query bottle data either by cruise ID(s) or by specifying spatiotemporal bounds:

* `lon0`, `lat0`: starting longitude and latitude (required if `cruise` is not specified).
* `lon1`, `lat1`: optional ending coordinates to define a bounding box.
* `dep0`, `dep1`: depth range filters.
* `start`, `end`: time range filters.
* `cruise`: comma-separated list of expocodes (e.g. `cruise=21OR19910626`), required if `lon0`, `lat0` are not used.
* `append`: output variables to include (e.g. `append=temperature,salinity,oxygen`). Full list of available data variables: https://gee-community-catalog.org/projects/glodap/#variable-key
* `flag`: enable to corresponding World Ocean Circulation Experiment (WOCE) flag of appended varaibles (default: false).
* `qc`: enable to corresponding quality control (QC) flag of appended varaibles (default: false).
* `format`: output format (`json` or `csv`).

📌 **Examples:**

* Retrieve temperature and salinity data for a specific cruise in CSV:

  ```
  https://ecodata.odb.ntu.edu.tw/glodap/v2/2023?cruise=21OR19910626&append=temperature,salinity&format=csv
  ```

* Retrieve nutrient, oxygen, and chlorofluorocarbon data for a given spatial, depth, and time range:

  ```
  https://ecodata.odb.ntu.edu.tw/glodap/v2/2023?lon0=-60&lon1=-30&lat0=0&lat1=30&dep0=0&dep1=200&start=1980-01-01&end=2020-12-31&append=nitrate,silicate,phosphate,oxygen,cfc*
  ```

📌 **Tip:** Most filters support case-insensitive matching and wildcards (`*`). Use the `/cruise` endpoint to discover valid `expocode` identifiers for cruise selection.

### Demo 

[![Demo_by_ODB GLODAP_API](https://github.com/cywhale/ODB/blob/master/img/GLODAP_bottle_proflies_OR1-287_ODB.png?raw=true)](https://github.com/cywhale/ODB/blob/master/img/GLODAP_bottle_proflies_OR1-287_ODB.png)<br/>
*The reproducible code is at my repo: https://github.com/cywhale/woa23/blob/main/dev/sim_woa23_api03_vs_GLODAP_CTD.ipynb*

 
### Attribution

[GLODAP official website](https://glodap.info/)

* Data Citation

```
    Olsen, A., R. M. Key, S. van Heuven, S. K. Lauvset, A. Velo, X. Lin, C. Schirnick, A. Kozyr, T. Tanhua, M. Hoppema, S. Jutterström, R. Steinfeldt, E. Jeansson, M. Ishii, F. F. Pérez and T. Suzuki. The Global Ocean Data Analysis Project version 2 (GLODAPv2) – an internally consistent data product for the world ocean, Earth Syst. Sci. Data, 8, 297–323, 2016, doi:10.5194/essd-8-297-2016
```

```
    Key, R.M., A. Olsen, S. van Heuven, S. K. Lauvset, A. Velo, X. Lin, C. Schirnick, A. Kozyr, T. Tanhua, M. Hoppema, S. Jutterström, R. Steinfeldt, E. Jeansson, M. Ishii, F. F. Perez, and T. Suzuki. 2015. Global Ocean Data Analysis Project, Version 2 (GLODAPv2), ORNL/CDIAC-162, NDP-093. Carbon Dioxide Information Analysis Center, Oak Ridge National Laboratory, US Department of Energy, Oak Ridge, Tennessee. doi:10.3334/CDIAC/OTG.NDP093_GLODAPv2
```

### API Citation

* This API is compiled by [Ocean Data Bank](https://www.odb.ntu.edu.tw) (ODB), and can be cited as:

    * Ocean Data Bank, National Science and Technology Council, Taiwan. https://doi.org/10.5281/zenodo.7512112. Accessed DAY/MONTH/YEAR from ecodata.odb.ntu.edu.tw/api/glodap/v2/2023. v1.0.
