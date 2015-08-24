<xsl:stylesheet version="2.0" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:gml="http://www.opengis.net/gml" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:fn="http://www.w3.org/2005/xpath-functions" exclude-result-prefixes="xsl xsi fn imgeo xlink">

    <xsl:output method="xml" encoding="UTF-8" omit-xml-declaration="yes"/>

    <xsl:strip-space elements="*"/>

    <xsl:template match="gml:MultiSurface">
        <gml:MultiSurface>
            <xsl:for-each select=".//gml:surfaceMember">
                <gml:surfaceMember>
                    <xsl:apply-templates select="node()"/>
                </gml:surfaceMember>
            </xsl:for-each>
        </gml:MultiSurface>
    </xsl:template> 

    <xsl:template match="gml:MultiPoint">
        <gml:MultiPoint>
            <xsl:for-each select=".//gml:Point">
                <gml:pointMember>
                    <gml:Point>
                        <gml:pos>
                            <xsl:value-of select="gml:pos"/>
                        </gml:pos>
                    </gml:Point>
                </gml:pointMember>
            </xsl:for-each>
        </gml:MultiPoint>
    </xsl:template>
	
    <xsl:template match="gml:Polygon|gml:PolygonPatch">
        <gml:Polygon>
            <xsl:choose>
                <xsl:when test="gml:exterior/gml:Ring">
                    <gml:exterior>
                        <gml:Ring>
                            <gml:curveMember>
                                <gml:Curve>
                                    <gml:segments>
                                        <xsl:apply-templates select="gml:exterior/gml:Ring/gml:curveMember/gml:Curve/gml:segments/*"/>
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
                                <xsl:value-of select=".//gml:exterior//gml:posList"/>
                            </gml:posList>
                        </gml:LinearRing>
                    </gml:exterior>
                </xsl:otherwise>
            </xsl:choose>
            <xsl:for-each select=".//gml:interior">
                <xsl:choose>
                    <xsl:when test="gml:Ring">
                        <gml:interior>
                            <gml:Ring>
                                <gml:curveMember>
                                    <gml:Curve>
                                        <gml:segments>
                                            <xsl:apply-templates select="gml:Ring/gml:curveMember/gml:Curve/gml:segments/*"/>
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
                                    <xsl:value-of select=".//gml:posList"/>
                                </gml:posList>
                            </gml:LinearRing>
                        </gml:interior>
                    </xsl:otherwise>
                </xsl:choose>
            </xsl:for-each>
        </gml:Polygon>
    </xsl:template>

    <xsl:template match="gml:LineStringSegment">
        <gml:LineStringSegment>
            <gml:posList>
                <xsl:value-of select=".//gml:posList"/>
            </gml:posList>
        </gml:LineStringSegment>
    </xsl:template>

    <xsl:template match="gml:Arc">
        <gml:Arc>
            <gml:posList>
                <xsl:value-of select=".//gml:posList"/>
            </gml:posList>
        </gml:Arc>		
    </xsl:template>

    <xsl:template match="gml:LineString">
        <gml:LineString>
            <gml:posList>
                <xsl:value-of select=".//gml:posList"/>
            </gml:posList>
        </gml:LineString>
    </xsl:template>

    <xsl:template match="gml:Curve">
        <gml:Curve>
            <gml:segments-l>
                <xsl:apply-templates select=".//gml:Arc|.//gml:LineStringSegment"/>
            </gml:segments-l>
        </gml:Curve>
    </xsl:template>

    <xsl:template match="gml:Point">
        <gml:Point>
            <gml:pos>
                <xsl:value-of select="gml:pos"/>
            </gml:pos>
        </gml:Point>
    </xsl:template>

</xsl:stylesheet>
