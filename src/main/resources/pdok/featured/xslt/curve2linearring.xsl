<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="2.0" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:imgeo-s="http://www.geostandaarden.nl/imgeo/2.1/simple/gml31" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:fn="http://www.w3.org/2005/xpath-functions" exclude-result-prefixes="xsl xsi fn imgeo">

    <xsl:output method="xml" encoding="UTF-8"/>

    <xsl:strip-space elements="*"/>

    <xsl:template match="Ring">
             <LinearRing>
                <posList> 
                   <xsl:for-each select=".//curveMember/Curve/segments/*|.//curveMember/LineString">
                         <xsl:value-of select=".//posList" />
                         <xsl:text> </xsl:text>
                   </xsl:for-each>
                </posList>
            </LinearRing>
    </xsl:template>

    <xsl:template match="Curve">
             <LineStringSegment>
                <posList> 
                   <xsl:for-each select="//segments/*">
                         <xsl:value-of select=".//posList" />
                         <xsl:text> </xsl:text>
                   </xsl:for-each>
                </posList>
            </LineStringSegment>
    </xsl:template>

    <xsl:template match="@*|node()">
        <xsl:copy>
            <xsl:apply-templates select="@*|node()"/>
        </xsl:copy>
    </xsl:template>

</xsl:stylesheet>
