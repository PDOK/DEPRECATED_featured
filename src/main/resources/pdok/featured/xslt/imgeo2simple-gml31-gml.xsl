<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="2.0" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:gml="http://www.opengis.net/gml" xmlns:imgeo-s="http://www.geostandaarden.nl/imgeo/2.1/simple/gml31" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:fn="http://www.w3.org/2005/xpath-functions" exclude-result-prefixes="xsl xsi fn imgeo">

    <xsl:output method="xml" encoding="UTF-8"/>

    <xsl:strip-space elements="*"/>

    <xsl:template match="MultiSurface">
        <gml:MultiSurface>
            <xsl:for-each select=".//surfaceMember">
                <gml:surfaceMember>
                    <xsl:apply-templates select="node()"/>
                </gml:surfaceMember>
            </xsl:for-each>
        </gml:MultiSurface>
    </xsl:template>

    <xsl:template match="MultiPoint">
        <gml:MultiPoint>
            <xsl:for-each select=".//Point">
                <gml:pointMember>
                    <gml:Point>
                        <gml:pos>
                            <xsl:value-of select="pos"/>
                        </gml:pos>
                    </gml:Point>
                </gml:pointMember>
            </xsl:for-each>
        </gml:MultiPoint>
    </xsl:template>
	
    <xsl:template match="Polygon|PolygonPatch">
        <gml:Polygon>
            <xsl:choose>
                <xsl:when test="exterior/Ring">
                    <gml:exterior>
                        <gml:Ring>
                            <gml:curveMember>
                                <gml:Curve>
                                    <gml:segments>
                                        <xsl:apply-templates select="exterior/Ring/curveMember/Curve/segments/*"/>
                                    </gml:segments>
                                </gml:Curve>
                            </gml:curveMember>
                        </gml:Ring>
                    </gml:exterior>
                </xsl:when>
                <xsl:otherwise>
                    <gml:exterior>
                        <gml:LinearRing>
                            <gml:posList>
                                <xsl:value-of select=".//exterior//posList"/>
                            </gml:posList>
                        </gml:LinearRing>
                    </gml:exterior>
                </xsl:otherwise>
            </xsl:choose>
            <xsl:for-each select=".//interior">
                <xsl:choose>
                    <xsl:when test="Ring">
                        <gml:interior>
                            <gml:Ring>
                                <gml:curveMember>
                                    <gml:Curve>
                                        <gml:segments>
                                            <xsl:apply-templates select="Ring/curveMember/Curve/segments/*"/>
                                        </gml:segments>
                                    </gml:Curve>
                                </gml:curveMember>
                            </gml:Ring>
                        </gml:interior>
                    </xsl:when>
                    <xsl:otherwise>
                        <gml:interior>
                            <gml:LinearRing>
                                <gml:posList>
                                    <xsl:value-of select=".//posList"/>
                                </gml:posList>
                            </gml:LinearRing>
                        </gml:interior>
                    </xsl:otherwise>
                </xsl:choose>
            </xsl:for-each>
        </gml:Polygon>
    </xsl:template>

    <xsl:template match="LineStringSegment">
        <gml:LineStringSegment>
            <gml:posList>
                <xsl:value-of select=".//posList"/>
            </gml:posList>
        </gml:LineStringSegment>
    </xsl:template>

    <xsl:template match="Arc">
        <gml:Arc>
            <gml:posList>
                <xsl:value-of select=".//posList"/>
            </gml:posList>
        </gml:Arc>		
    </xsl:template>

    <xsl:template match="LineString">
        <gml:LineString>
            <gml:posList>
                <xsl:value-of select=".//posList"/>
            </gml:posList>
        </gml:LineString>
    </xsl:template>

    <xsl:template match="Curve">
        <gml:Curve>
            <gml:segments>
                <xsl:apply-templates select=".//Arc|.//LineStringSegment"/>
            </gml:segments>
        </gml:Curve>
    </xsl:template>

    <xsl:template match="Point">
        <gml:Point>
            <gml:pos>
                <xsl:value-of select="pos"/>
            </gml:pos>
        </gml:Point>
    </xsl:template>

</xsl:stylesheet>
