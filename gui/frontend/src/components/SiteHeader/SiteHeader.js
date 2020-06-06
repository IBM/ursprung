import React from "react";
import {
  Header,
  HeaderName,
  HeaderNavigation,
  HeaderGlobalBar,
  SkipToContent,
} from "carbon-components-react/lib/components/UIShell";

const SiteHeader = () => (
  <Header aria-label="Ursprung Provenance GUI">
    <SkipToContent />
    <HeaderName href="/" prefix="IBM">
      Ursprung Provenance GUI
    </HeaderName>
    <HeaderNavigation aria-label="Ursprung Provenance GUI">
    </HeaderNavigation>
    <HeaderGlobalBar />
  </Header>
);
export default SiteHeader;