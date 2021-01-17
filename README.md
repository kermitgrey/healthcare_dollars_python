# healthcaredollars_python

<p> The Centers for Medicare and Medicaid (CMS) has developed a system to
    track any payments (or transfers-of-value) that arise between pharmaceutical
    companies (and others) and physicians. CMS makes the raw data available to the
    public on its website for those interested in making use of it.  I have 
    utilized a subset of CMS' data and prepared an analysis that describes the 
    nature of those financial relationships.</p>
    
<p> This repo contains the following files:</p>

<ol>
    <li> Health Care Dollars - Python.ipynb: the analysis of Open Payments data</li>
    <li> Python code used to generate the data underlying the analysis, including:</li>
<ul>
<li> generateDoctors.py - takes the data from CMS' Physician Profile Supplement
    and generates an extract of physician identifying information.</li>
<li> generatePayments.py - loads the General Payments data sets and transforms
    the data as needed.</li>
<li> generatePayors.py - generates an extract of all the companies making payments.</li>
<li> generatePivot.py - takes the data created by generatePayments.py and creates a
    pivot of all the data.</li>
    <li> visualCode.py - generates all the visualizations present in the analysis.</li>
</ul>
</ol>

             

<p>This analysis makes use of several data sets, including:</p>

<ul>
    <li> General Payments data set - contains all transactions detailing payments
        made by a company to a physician.</li>
    <li> Physician Profile Supplement - contains all identifying 
        information for a physician indicated as a recipient in Open Payments.</li>
    <li> Health Care Provider Taxonomy data set - identifies a health care provider's
        specialty category.</li>
</ul>
    
<p>The source data files can be dowloaded from their respective website source:</p>

<ul>
<li>General Payments Detail and the Physician Profile
Supplement all from CMS at <link>https://www.cms.gov/OpenPayments/Explore-the-Data/Dataset-Downloads</link></li>
<li>Health Care Provider Taxonomy from the National Uniform Claim
Committee at <link> https://nucc.org/index.php/code-sets-mainmenu-41/provider-taxonomy-mainmenu-40/csv-mainmenu-57</link>
</li>
</ul>
