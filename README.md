<h1 class="code-line" data-line-start=0 data-line-end=1 ><a id="csv_to_parquet_and_parquet_to_csv_converter_0"></a>csv to parquet and parquet to csv converter</h1>
<h2 class="code-line" data-line-start=1 data-line-end=2 ><a id="10000ft_Overview_1"></a>10000ft. Overview</h2>
This tool is able to convert .csv files to .parquet files used for columnar storage typically in the Hadoop ecosystem. It is also able to
convert .parquet files to .csv files. This is achieved thanks to the 4 built-in Pandas dataframe methods <code>read_csv</code>, <code>read_parquet</code>, <code>to_csv</code> and <code>to_parquet</code>.
Because Pandas uses <code>s3fs</code> for AWS S3 integration, so you are free to choose whether the location of the source and/or converted target files is on your local machine or in AWS S3.
<h2 class="code-line" data-line-start=3 data-line-end=4 ><a id="How_to_install_and_run_3"></a>How to install and run</h2>
<ol>
<li class="has-line-data" data-line-start="10" data-line-end="11">
<p class="has-line-data" data-line-start="10" data-line-end="11">setup and activate a virtual environment</p>
</li>
<li class="has-line-data" data-line-start="4" data-line-end="5">
<p class="has-line-data" data-line-start="4" data-line-end="5">pip3 install -r requirements.txt</p>
</li>
<li class="has-line-data" data-line-start="5" data-line-end="10">
<p class="has-line-data" data-line-start="5" data-line-end="10">in case you wish to use AWS S3 as a source file and/or a target file location for the conversion,<br>
set environment variables like:<br>
<code>aws_access_key_id = &lt;your AWS IAM access key id&gt;</code><br>
<code>aws_secret_access_key = &lt;your AWS IAM secret access key value&gt;</code><br>
Pandas uses s3fs to integrate with AWS S3, please see <a href="https://s3fs.readthedocs.io/en/latest/">https://s3fs.readthedocs.io/en/latest/</a> in case of any authentication issues.</p>
</li>
<li class="has-line-data" data-line-start="11" data-line-end="48">
<p class="has-line-data" data-line-start="11" data-line-end="12">run <code>python __main__.py</code> with the requiered arguments <code>-sfp</code> for source file path, <code>-tfp</code> for target file path, set like:</p>
<h2 class="code-line" data-line-start=12 data-line-end=13 ><a id="for_csv_to_parquet_conversion_12"></a>for csv to parquet conversion:</h2>
<h4 class="code-line" data-line-start=14 data-line-end=15 ><a id="local_csv_file_to_local_parquet_file_14"></a>local csv file to local parquet file:</h4>
<p class="has-line-data" data-line-start="15" data-line-end="17"><code>-sfp C:\your local folder\source file name.csv</code><br>
<code>-tfp C:\your local folder\target file name.parquet</code></p>
<h4 class="code-line" data-line-start=18 data-line-end=19 ><a id="local_csv_file_to_s3_parquet_file_18"></a>local csv file to s3 parquet file:</h4>
<p class="has-line-data" data-line-start="19" data-line-end="21"><code>-sfp C:\your local folder\source file name.csv</code><br>
<code>-tfp s3://your bucket name/your bucket &quot;folder&quot; prefix/target file name.parquet</code></p>
<h4 class="code-line" data-line-start=22 data-line-end=23 ><a id="s3_csv_file_to_local_parquet_file_22"></a>s3 csv file to local parquet file:</h4>
<p class="has-line-data" data-line-start="23" data-line-end="25"><code>-sfp s3://your bucket name/your bucket &quot;folder&quot; prefix/source.csv</code><br>
<code>-tfp C:\your local folder\target.parquet</code></p>
<h4 class="code-line" data-line-start=26 data-line-end=27 ><a id="s3_csv_file_to_s3_parquet_file_26"></a>s3 csv file to s3 parquet file:</h4>
<p class="has-line-data" data-line-start="27" data-line-end="29"><code>-sfp s3://your bucket name/your bucket &quot;folder&quot; prefix/source file name.csv</code><br>
<code>-tfp s3://your bucket name/your bucket &quot;folder&quot; prefix/target file name.parquet</code></p>
<h2 class="code-line" data-line-start=30 data-line-end=31 ><a id="for_parquet_to_csv_conversion_30"></a>for parquet to csv conversion:</h2>
<h4 class="code-line" data-line-start=32 data-line-end=33 ><a id="local_parquet_file_to_local_csv_file_32"></a>local parquet file to local csv file:</h4>
<p class="has-line-data" data-line-start="33" data-line-end="35"><code>-sfp C:\your local folder\source file name.parquet</code><br>
<code>-tfp C:\your local folder\target file name.csv</code></p>
<h4 class="code-line" data-line-start=36 data-line-end=37 ><a id="local_parquet_file_to_s3_csv_file_36"></a>local parquet file to s3 csv file:</h4>
<p class="has-line-data" data-line-start="37" data-line-end="39"><code>-sfp C:\your local folder\source file name.parquet</code><br>
<code>-tfp s3://your bucket name/your bucket &quot;folder&quot; prefix/target file name.csv</code></p>
<h4 class="code-line" data-line-start=40 data-line-end=41 ><a id="s3_parquet_file_to_local_csv_file_40"></a>s3 parquet file to local csv file:</h4>
<p class="has-line-data" data-line-start="41" data-line-end="43"><code>-sfp s3://your bucket name/your bucket &quot;folder&quot; prefix/source file name.parquet</code><br>
<code>-tfp C:\your local folder\target.csv</code></p>
<h4 class="code-line" data-line-start=44 data-line-end=45 ><a id="s3_parquet_file_to_s3_csv_file_44"></a>s3 parquet file to s3 csv file:</h4>
<p class="has-line-data" data-line-start="45" data-line-end="47"><code>-sfp s3://your bucket name/your bucket &quot;folder&quot; prefix/source file name.parquet</code><br>
<code>-tfp s3://your bucket name/your bucket &quot;folder&quot; prefix/target file name.csv</code></p>
</li>
<li class="has-line-data" data-line-start="48" data-line-end="52">
<p class="has-line-data" data-line-start="48" data-line-end="51">you can add these optional arguments:<br>
<code>-cols</code> argument is used to define a subset of columns from the source file, meaning that only the columns passed as a <code>list</code> to this argument will get loaded and converted, example: <code>[&quot;column_name_1&quot;, &quot;column_name_2&quot;]</code><br>
<code>-comp</code> argument is used for overriding the default parquet compression type (<code>snappy</code>) in case of converting from a csv to parquet file, example: <code>gzip</code></p>
</li>
</ol>
<h2 class="code-line" data-line-start=52 data-line-end=53 ><a id="How_to_verify_a_parquet_file_52"></a>How to verify a parquet file:</h2>
<ul><li>http://parquet-viewer-online.com/</li></ul>
<h2 class="code-line" data-line-start=55 data-line-end=56 ><a id="Further_documentation_55"></a>Further documentation</h2>
<ul>
  <li>https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html</li>
  <li>https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_parquet.html</li>
  <li>https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_csv.html</li>
  <li>https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_parquet.html</li>
  <li>https://s3fs.readthedocs.io/en/latest/</li>
</ul>
