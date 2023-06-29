import React from 'react';
import styled from 'styled-components';

const StyledMainContent = styled.main`
  display: flex;
  justify-content: space-around;
  background-color: #222;
  color: #fff;
  padding: 20px;
`;

const ContentBlock = styled.div`
  background-color: #444;
  padding: 20px;
  margin: 10px;
  flex: 1;
`;

const MainContent = () => {
  return (
    <StyledMainContent>
      <ContentBlock>Upcoming Matches</ContentBlock>
      <ContentBlock>Main Section</ContentBlock>
      <ContentBlock>Results</ContentBlock>
    </StyledMainContent>
  );
}

export default MainContent;