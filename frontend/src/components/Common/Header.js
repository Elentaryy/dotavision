import React from 'react';
import styled from 'styled-components';

const StyledHeader = styled.header`
  display: flex;
  justify-content: space-between;
  align-items: center;
  background-color: #333;
  color: #fff;
  padding: 20px;
`;

const Logo = styled.div`
  width: 60px;
  height: 60px;
  background-color: #fff;
`;

const Nav = styled.nav`
  & > a {
    margin-right: 20px;
    color: #fff;
    text-decoration: none;
  }
`;

const Header = () => {
  return (
    <StyledHeader>
      <Logo />
      <Nav>
        <a href="/">Home</a>
        <a href="/stats">Stats</a>
        <a href="/predictions">Predictions</a>
      </Nav>
    </StyledHeader>
  );
}

export default Header;