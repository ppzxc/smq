<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:pmd="http://pmd.sourceforge.net/report/2.0.0" exclude-result-prefixes="pmd">
  <xsl:output method="html" encoding="UTF-8"/>
  <xsl:template match="/">
    <html><head><title>PMD report</title></head><body>
      <h1>PMD report</h1>
      <p>Violations: <xsl:value-of select="count(pmd:pmd/pmd:file/pmd:violation)"/></p>
      <table>
        <thead><tr><th>Rule</th><th>File</th><th>Line</th><th>Problem</th></tr></thead>
        <tbody>
          <xsl:for-each select="pmd:pmd/pmd:file/pmd:violation">
            <tr>
              <td><a href="{@externalInfoUrl}"><xsl:value-of select="@rule"/></a></td>
              <td><xsl:value-of select="../@name"/></td>
              <td><xsl:value-of select="@beginline"/></td>
              <td><xsl:value-of select="normalize-space(.)"/></td>
            </tr>
          </xsl:for-each>
        </tbody>
      </table>
    </body></html>
  </xsl:template>
</xsl:stylesheet>
