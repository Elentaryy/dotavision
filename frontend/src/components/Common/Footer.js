import React from 'react';
import styled from 'styled-components';

const StyledFooter = styled.footer`
  background-color: #333;
  color: #fff;
  text-align: center;
  padding: 20px;
`;

const Footer = () => {
  return (
    <StyledFooter>
      <p>Contact Us: contact@dotavision.com</p>
      <p>Support: support@dotavision.com</p>
      <p>Donate: donate@dotavision.com</p>
      <p>Advertising: advertise@dotavision.com</p>
    </StyledFooter>
  );
}

export default Footer;