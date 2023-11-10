# EMR
Tools for exploring EMR resources, impact, etc.

## phd_impact
usage:
```bash
./phd_impact.py --days 3 phd_output.csv emr_output.csv
```
This script is essentially a needles/haystack filter.
Both files are expected to be CSVs with one EMR cluster per line.
There are configuration values in the script for which column contains the ARN.
For each cluster in the first file, we look for it in the second and confirm that it is terminated,
and that the termination was at least <days> days ago. If it does not match both criteria, it will be reported.

Use this script to confirm you have terminated clusters which are reported impacted in PHD. 
For example, if PHD reports a version deprecation and you recently updated your clusters, 
you may still see reported impact. You can use this script to confirm.

Clusters either unterminated or terminated recently are printed to stdout (`... > impacted.csv`) while notices/info are
printed to stderr. 
