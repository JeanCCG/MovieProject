// src/components/NewsFeed.js

import React, { useState, useEffect } from 'react';
import { useLocation } from 'react-router-dom';
//import axios from 'axios';
import './NewsFeed.css';
import { suggestMoviesBasedOnActorInMovie, RecommendMovieGenre } from './api';

const buttonStyle = {
  backgroundColor: '#4caf50',
  color: 'white',
  padding: '10px 15px',
  border: 'none',
  borderRadius: '5px',
  cursor: 'pointer',
  margin: '0 5px',
};

function NewsFeed({ news }) {
  const location = useLocation();
  const userData = location.state ? location.state : null;
  const [listRecommendedMovie, setRecommendedMovie] = useState(null);
  const [listSugMoviesBasedOnActorInMovie, setSugMoviesBasedOnActorInMovie] = useState(null);

  const handleSuggestMoviesBasedOnActorInMovie = async () => {
    try {
      const response = await suggestMoviesBasedOnActorInMovie(userData.user.name);
      setSugMoviesBasedOnActorInMovie(response);
    } catch (error) {
      console.error('Error during login:', error);
    }
  };

  const handleRecommendedMovieByGenre = async () => {
    try {
      const response = await RecommendMovieGenre(userData.user.name);
      setRecommendedMovie(response);
    } catch (error) {
      console.error('Error during login:', error);
    }
  };

  useEffect(() => {
    // Call handleUserSuggest when the component mounts
    handleRecommendedMovieByGenre();
    handleSuggestMoviesBasedOnActorInMovie();
  }, []); 

  return (
    <div className="news-feed">
            {listRecommendedMovie && listRecommendedMovie.length > 0 && (
        <div className="news-feed">
          <h3>For you: {listRecommendedMovie[0].FavoriteGenre}</h3>
          <div className="column-container">
            {listRecommendedMovie.map((user, index) => (
              <div key={index} className="news-item">
                <p>{user.title} ({user.year})</p>
                <p>{user.plot}</p>
                <img
                  src={user.poster}
                  alt={user.title}
                  style={{ maxWidth: '20%', height: 'auto' }}
                />
                <p>Directed by: {user.director}</p>
              </div>
            ))}
          </div>
        </div>
      )}

      {listSugMoviesBasedOnActorInMovie && (
      <div className="news-feed">
        <h3>Movie Suggestions</h3>
        <div className="column-container">
        {listSugMoviesBasedOnActorInMovie.map((user, index) => (
          <div key={index} className="news-item">
            <h4>Because you loved {user.Actor} in {user.MyRatedMovie}</h4>
            <p>{user.RecommendedMovie} ({user.Year})</p>
            <p>{user.Plot}</p>
            <img src={user.Poster} alt={user.RecommendedMovie} style={{ maxWidth: '10%', height: 'auto' }} />
            <p>Directed by: {user.Director}</p>
          </div>
        ))}
      </div>
      </div>
    )}

    </div>
  );
}

export default NewsFeed;
